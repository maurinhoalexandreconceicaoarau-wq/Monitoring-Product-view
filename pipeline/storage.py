"""Small local SQLite store for durable, queryable Spark outputs."""
import os
import sqlite3
from contextlib import contextmanager
from pathlib import Path
from threading import Lock

from .common import ARTIFACTS


DATABASE_PATH = Path(os.environ.get("ANALYTICS_DB_PATH", ARTIFACTS / "retailrocket_analytics.db"))
_init_lock = Lock()
_initialized = False


def connect():
    DATABASE_PATH.parent.mkdir(parents=True, exist_ok=True)
    connection = sqlite3.connect(DATABASE_PATH, timeout=15)
    connection.row_factory = sqlite3.Row
    connection.execute("PRAGMA busy_timeout=15000")
    return connection


@contextmanager
def database_connection():
    connection = connect()
    try:
        yield connection
        connection.commit()
    except Exception:
        connection.rollback()
        raise
    finally:
        connection.close()


def initialize_database():
    global _initialized
    if _initialized:
        return
    with _init_lock:
        if _initialized:
            return
        with database_connection() as connection:
            connection.execute("PRAGMA journal_mode=WAL")
            connection.executescript("""
            CREATE TABLE IF NOT EXISTS processed_events (
                event_id TEXT PRIMARY KEY,
                event_time TEXT,
                ingested_at TEXT,
                processed_at TEXT,
                visitor_id TEXT,
                session_id TEXT,
                event_type TEXT NOT NULL,
                item_id TEXT,
                transaction_id TEXT,
                source TEXT,
                capture_id TEXT,
                kafka_partition INTEGER,
                kafka_offset INTEGER,
                spark_batch_id INTEGER,
                preprocessing_status TEXT NOT NULL DEFAULT 'valid',
                stored_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ','now'))
            );
            CREATE INDEX IF NOT EXISTS idx_processed_events_time ON processed_events(event_time);
            CREATE INDEX IF NOT EXISTS idx_processed_events_type ON processed_events(event_type);
            CREATE INDEX IF NOT EXISTS idx_processed_events_item ON processed_events(item_id);
            CREATE INDEX IF NOT EXISTS idx_processed_events_session ON processed_events(session_id);

            CREATE TABLE IF NOT EXISTS spark_batches (
                stream_id TEXT NOT NULL,
                batch_id INTEGER NOT NULL,
                records_received INTEGER NOT NULL,
                valid_records INTEGER NOT NULL,
                invalid_records INTEGER NOT NULL,
                duplicate_records INTEGER NOT NULL,
                duration_seconds REAL,
                completed_at TEXT,
                PRIMARY KEY (stream_id, batch_id)
            );
            """)
        _initialized = True


def store_processed_events(events):
    """Insert Spark-committed rows once; replayed event IDs remain idempotent."""
    rows = []
    for event in events:
        event_id = event.get("event_id")
        event_type = event.get("event_type")
        if event_id is None or event_type not in ("view", "addtocart", "transaction"):
            continue
        rows.append((
            str(event_id), event.get("event_time"), event.get("ingested_at"), event.get("processed_at"),
            event.get("visitor_id"), event.get("session_id"), event_type, event.get("item_id"),
            event.get("transaction_id"), event.get("source"), event.get("capture_id"),
            event.get("kafka_partition"), event.get("kafka_offset"), event.get("spark_batch_id"),
            event.get("preprocessing_status", "valid"),
        ))
    if not rows:
        return 0
    initialize_database()
    with database_connection() as connection:
        before = connection.total_changes
        connection.executemany("""
            INSERT OR IGNORE INTO processed_events (
                event_id,event_time,ingested_at,processed_at,visitor_id,session_id,event_type,item_id,
                transaction_id,source,capture_id,kafka_partition,kafka_offset,spark_batch_id,preprocessing_status
            ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
        """, rows)
        return connection.total_changes - before


def store_spark_batch(stream_id, batch):
    if not batch or batch.get("id") is None:
        return
    initialize_database()
    with database_connection() as connection:
        connection.execute("""
            INSERT INTO spark_batches (
                stream_id,batch_id,records_received,valid_records,invalid_records,
                duplicate_records,duration_seconds,completed_at
            ) VALUES (?,?,?,?,?,?,?,?)
            ON CONFLICT(stream_id,batch_id) DO UPDATE SET
                records_received=excluded.records_received,
                valid_records=excluded.valid_records,
                invalid_records=excluded.invalid_records,
                duplicate_records=excluded.duplicate_records,
                duration_seconds=excluded.duration_seconds,
                completed_at=excluded.completed_at
        """, (
            str(stream_id or "unknown"), int(batch["id"]), int(batch.get("received", 0) or 0),
            int(batch.get("valid", 0) or 0), int(batch.get("invalid", 0) or 0),
            int(batch.get("duplicates", 0) or 0), batch.get("processing_seconds"), batch.get("at"),
        ))


def database_status():
    initialize_database()
    with database_connection() as connection:
        count = int(connection.execute("SELECT COUNT(*) FROM processed_events").fetchone()[0])
        visitors = int(connection.execute("SELECT COUNT(DISTINCT visitor_id) FROM processed_events").fetchone()[0])
        products = int(connection.execute("SELECT COUNT(DISTINCT item_id) FROM processed_events").fetchone()[0])
        type_counts = {row["event_type"]: int(row["count"]) for row in connection.execute(
            "SELECT event_type,COUNT(*) AS count FROM processed_events GROUP BY event_type ORDER BY event_type")}
        batch_count = int(connection.execute("SELECT COUNT(*) FROM spark_batches").fetchone()[0])
        latest = connection.execute("SELECT MAX(processed_at) FROM processed_events").fetchone()[0]
    return {"status": "available", "path": str(DATABASE_PATH), "processed_events": count,
            "unique_visitors": visitors, "unique_products": products,
            "spark_batches": batch_count, "event_types": type_counts, "last_processed_at": latest,
            "size_bytes": DATABASE_PATH.stat().st_size if DATABASE_PATH.exists() else 0}


def query_events(limit=500, event_type=None, product_id=None):
    initialize_database()
    limit = max(1, min(int(limit), 5000))
    clauses, params = [], []
    if event_type in ("view", "addtocart", "transaction"):
        clauses.append("event_type=?")
        params.append(event_type)
    if product_id:
        clauses.append("item_id=?")
        params.append(str(product_id).strip())
    where = " WHERE " + " AND ".join(clauses) if clauses else ""
    with database_connection() as connection:
        return [dict(row) for row in connection.execute(
            f"SELECT event_id,event_time,ingested_at,processed_at,visitor_id,session_id,event_type,item_id,transaction_id,source,capture_id,kafka_partition,kafka_offset,spark_batch_id,preprocessing_status,stored_at FROM processed_events{where} ORDER BY processed_at DESC,event_id LIMIT ?",
            [*params, limit])]


def query_batches(limit=100):
    initialize_database()
    with database_connection() as connection:
        return [dict(row) for row in connection.execute(
            "SELECT stream_id,batch_id,records_received,valid_records,invalid_records,duplicate_records,duration_seconds,completed_at FROM spark_batches ORDER BY completed_at DESC LIMIT ?",
            (max(1, min(int(limit), 1000)),))]
