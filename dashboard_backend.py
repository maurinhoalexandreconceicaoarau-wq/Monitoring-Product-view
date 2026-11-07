"""Shared analytics and replay controls for the Streamlit dashboard."""
from collections import Counter, defaultdict, deque
import csv
import io
import json
import math
import os
import subprocess
import sys
import socket
import threading
import time
import uuid
from threading import RLock
from datetime import datetime, timezone

from pipeline.common import ARTIFACTS, DATA, METRICS, REPLAY_STATE, SPARK_EVENTS, SPARK_HEARTBEAT, SPARK_STATE, read_json, write_json
from pipeline.storage import database_status, query_batches, query_events, store_processed_events, store_spark_batch

_cache_lock = RLock()
_cache = {"files": set(), "counts": Counter(), "visitors": set(), "items": set(), "event_ids": set(), "duplicate_rows": 0, "sessions": {},
          "products": defaultdict(Counter), "daily": defaultdict(Counter), "hourly": defaultdict(Counter), "recent": deque(maxlen=30),
          "batches": Counter(),
          "processed_event_details": {}, "capture_trace": deque(maxlen=2000), "capture_trace_position": 0,
          "capture_ack_counts": Counter(), "capture_processed_counts": Counter(), "database_pending_events": [], "total": 0}
_capture_lock = RLock()
_capture_process = None
_broker_cache_lock = RLock()
_broker_cache = {"checked_at": 0.0, "result": None}
_source_audit_lock = RLock()
_source_audit_thread = None
_database_error = None


def capture_is_running():
    global _capture_process
    with _capture_lock:
        if _capture_process is None:
            return False
        exit_code = _capture_process.poll()
        if exit_code is None:
            return True
        state = read_json(REPLAY_STATE, {})
        if state.get("status") == "running":
            write_json(REPLAY_STATE, {**state, "status": "error",
                "error": f"Replay producer stopped unexpectedly (exit {exit_code})."})
        return False


def current_processed_count():
    return int(read_json(METRICS, {}).get("processed_events", 0) or 0)


def kafka_status():
    host = os.getenv("KAFKA_HOST", "127.0.0.1")
    port = int(os.getenv("KAFKA_PORT", "9092"))
    now = time.monotonic()
    with _broker_cache_lock:
        if _broker_cache["result"] is not None and now - _broker_cache["checked_at"] < 5:
            return dict(_broker_cache["result"])
    try:
        with socket.create_connection((host, port), timeout=1):
            pass
        from kafka import KafkaConsumer, TopicPartition
        consumer = KafkaConsumer(bootstrap_servers=f"{host}:{port}", enable_auto_commit=False,
            request_timeout_ms=3000, api_version_auto_timeout_ms=3000, consumer_timeout_ms=1000)
        try:
            partitions = consumer.partitions_for_topic(os.getenv("KAFKA_TOPIC", "retailrocket-events")) or set()
            topic = os.getenv("KAFKA_TOPIC", "retailrocket-events")
            topic_partitions = [TopicPartition(topic, partition) for partition in sorted(partitions)]
            if topic_partitions:
                consumer.assign(topic_partitions)
                beginning = consumer.beginning_offsets(topic_partitions)
                ending = consumer.end_offsets(topic_partitions)
                retained_messages = sum(max(0, ending[p] - beginning[p]) for p in topic_partitions)
            else:
                beginning, ending, retained_messages = {}, {}, 0
            result = {"status": "reachable", "host": host, "port": port, "topic": topic,
                "topic_found": bool(topic_partitions), "partitions": sorted(partitions),
                "begin_offsets": {str(p.partition): beginning.get(p) for p in topic_partitions},
                "end_offsets": {str(p.partition): ending.get(p) for p in topic_partitions},
                "retained_messages": retained_messages, "checked_at": datetime.now(timezone.utc).isoformat()}
        finally:
            consumer.close(autocommit=False)
    except Exception as exc:
        result = {"status": "unreachable", "host": host, "port": port,
                  "error": str(exc), "checked_at": datetime.now(timezone.utc).isoformat()}
    with _broker_cache_lock:
        _broker_cache.update(checked_at=time.monotonic(), result=result)
    return dict(result)


def spark_status():
    state = read_json(SPARK_STATE, {"status": "not_started"})
    heartbeat = read_json(SPARK_HEARTBEAT, {})
    heartbeat_at = heartbeat.get("heartbeat_at")
    age_seconds = None
    if heartbeat_at:
        try:
            age_seconds = max(0.0, (datetime.now(timezone.utc) - datetime.fromisoformat(heartbeat_at)).total_seconds())
        except (TypeError, ValueError):
            age_seconds = None
    ttl = max(10, int(os.getenv("SPARK_HEARTBEAT_TTL_SECONDS", "20")))
    reported = state.get("status", "unknown")
    effective = reported
    if reported in ("running", "starting") and (age_seconds is None or age_seconds > ttl):
        effective = "stale"
    return {**state, "reported_status": reported, "status": effective,
            "heartbeat_at": heartbeat_at, "heartbeat_age_seconds": round(age_seconds, 1) if age_seconds is not None else None,
            "heartbeat_ttl_seconds": ttl}


def source_dataset_audit():
    """Return a cached CSV audit and run a one-time background scan when stale."""
    global _source_audit_thread
    if not DATA.is_file():
        return {"status": "missing", "source_records": None, "error": f"Dataset not found: {DATA.name}"}
    stat = DATA.stat()
    signature = {"source_size_bytes": stat.st_size, "source_mtime_ns": stat.st_mtime_ns}
    audit_path = ARTIFACTS / "source-audit.json"
    cached = read_json(audit_path, {})
    if all(cached.get(key) == value for key, value in signature.items()) and cached.get("status") == "completed":
        return cached
    with _source_audit_lock:
        if _source_audit_thread is None or not _source_audit_thread.is_alive():
            def scan():
                try:
                    missing = {}
                    event_types = Counter()
                    record_count = 0
                    with DATA.open(newline="", encoding="utf-8") as source:
                        reader = csv.DictReader(source)
                        columns = reader.fieldnames or []
                        missing = {column: 0 for column in columns}
                        for row in reader:
                            record_count += 1
                            for column in columns:
                                if row.get(column) is None or not str(row[column]).strip():
                                    missing[column] += 1
                            event_types[row.get("event", "").strip() or "missing"] += 1
                    write_json(audit_path, {**signature, "status": "completed", "source_records": record_count,
                        "missing_values": missing, "event_types": dict(event_types),
                        "completed_at": datetime.now(timezone.utc).isoformat()})
                except Exception as exc:
                    write_json(audit_path, {**signature, "status": "error", "error": str(exc),
                        "source_records": None, "completed_at": datetime.now(timezone.utc).isoformat()})
            _source_audit_thread = threading.Thread(target=scan, name="retailrocket-source-audit", daemon=True)
            _source_audit_thread.start()
    return {**cached, **signature, "status": "scanning", "source_records": cached.get("source_records")}


def dashboard_data():
    global _database_error
    with _cache_lock:
        if _cache["database_pending_events"]:
            try:
                store_processed_events(_cache["database_pending_events"])
                _cache["database_pending_events"].clear()
                _database_error = None
            except Exception as exc:
                _database_error = str(exc)
        event_files = list(SPARK_EVENTS.glob("part-*.json")) if SPARK_EVENTS.exists() else []
        for event_file in event_files:
            token = str(event_file.resolve())
            if token in _cache["files"]:
                continue
            # Spark commits immutable JSON part files. Read each once and retain
            # a compact running view, rather than rescanning millions of rows on
            # every three-second browser poll.
            try:
                database_events = []
                with event_file.open(encoding="utf-8") as source:
                    for line in source:
                        try: event = json.loads(line)
                        except ValueError: continue
                        database_events.append(event)
                        event_id = event.get("event_id")
                        if event_id is not None and event_id in _cache["event_ids"]:
                            _cache["duplicate_rows"] += 1
                            continue
                        if event_id is not None: _cache["event_ids"].add(event_id)
                        typ, sid, item = event.get("event_type"), event.get("session_id"), event.get("item_id")
                        _cache["total"] += 1; _cache["counts"][typ] += 1
                        if event.get("spark_batch_id") is not None:
                            _cache["batches"][int(event["spark_batch_id"])] += 1
                        day = (event.get("event_time") or "")[:10]
                        if len(day) == 10: _cache["daily"][day][typ] += 1
                        hour = (event.get("event_time") or "")[:13]
                        if len(hour) == 13: _cache["hourly"][hour][typ] += 1
                        if event.get("visitor_id") is not None: _cache["visitors"].add(event["visitor_id"])
                        if item is not None:
                            _cache["items"].add(item); _cache["products"][item][typ] += 1
                        s = _cache["sessions"].setdefault(sid, Counter())
                        s[typ] += 1
                        _cache["recent"].append({"event_id": event_id, "type": typ,
                            "event_time": event.get("event_time"), "ingested_at": event.get("ingested_at"),
                            "processed_at": event.get("processed_at"), "visitor_id": event.get("visitor_id"),
                            "status": "processed", "item_id": item,
                            "kafka_partition": event.get("kafka_partition"),
                            "kafka_offset": event.get("kafka_offset"),
                            "spark_batch_id": event.get("spark_batch_id"),
                            "preprocessing_status": event.get("preprocessing_status", "valid")})
                        if event_id is not None:
                            _cache["processed_event_details"][event_id] = {
                                "spark_batch_id": event.get("spark_batch_id"),
                                "spark_processed_at": event.get("processed_at"),
                                "preprocessing_status": event.get("preprocessing_status", "valid"),
                                "kafka_partition": event.get("kafka_partition"),
                                "kafka_offset": event.get("kafka_offset")}
                            if len(_cache["processed_event_details"]) > 5000:
                                _cache["processed_event_details"].pop(next(iter(_cache["processed_event_details"])))
                        capture_id = event.get("capture_id")
                        if capture_id:
                            _cache["capture_processed_counts"][capture_id] += 1
                try:
                    store_processed_events(database_events)
                    _database_error = None
                except Exception as exc:
                    _cache["database_pending_events"].extend(database_events)
                    _database_error = str(exc)
                _cache["files"].add(token)
            except OSError:
                continue  # Spark may still be committing the part file.
        # Backfill output committed before the database was enabled, or recover
        # writes that previously failed. The event_id primary key makes retries safe.
        try:
            if database_status()["processed_events"] < len(_cache["event_ids"]):
                for event_file in event_files:
                    try:
                        with event_file.open(encoding="utf-8") as source:
                            rows = [json.loads(line) for line in source if line.strip()]
                        store_processed_events(rows)
                    except (OSError, ValueError):
                        continue
            _database_error = None
        except Exception as exc:
            _database_error = str(exc)
        # Read newly appended Kafka acknowledgements once; this is the audit
        # trail that ties a requested source event to its broker offset.
        trace_path = ARTIFACTS / "capture-trace.jsonl"
        if trace_path.exists():
            try:
                # A cleared capture request can truncate this file while the
                # Streamlit process remains alive. Reset its in-memory cursor
                # too, otherwise the next request's trace may be skipped.
                if trace_path.stat().st_size < _cache["capture_trace_position"]:
                    _cache["capture_trace"].clear()
                    _cache["capture_ack_counts"].clear()
                    _cache["capture_processed_counts"].clear()
                    _cache["capture_trace_position"] = 0
                with trace_path.open(encoding="utf-8") as source:
                    source.seek(_cache["capture_trace_position"])
                    for line in source:
                        try:
                            traced_event = json.loads(line)
                            _cache["capture_trace"].append(traced_event)
                            if traced_event.get("capture_id") and traced_event.get("kafka_status") == "acknowledged":
                                _cache["capture_ack_counts"][traced_event["capture_id"]] += 1
                        except ValueError: continue
                    _cache["capture_trace_position"] = source.tell()
            except OSError:
                pass
        eligible_count = sum(1 for s in _cache["sessions"].values() if s["view"] or s["addtocart"])
        no_purchase_count = sum(1 for s in _cache["sessions"].values() if (s["view"] or s["addtocart"]) and not s["transaction"])
        counts, total = _cache["counts"].copy(), _cache["total"]
        visitors_count, items_count = len(_cache["visitors"]), len(_cache["items"])
        recent = list(_cache["recent"])
        top_products = [{"item_id": item, "views": c["view"], "cart_additions": c["addtocart"], "transactions": c["transaction"]}
                        for item, c in sorted(_cache["products"].items(), key=lambda pair: (sum(pair[1][k] for k in ("view", "addtocart", "transaction")), pair[1]["view"]), reverse=True)[:20]]
        product_leaders = {}
        for event_type, label, output_key in (("view", "Most viewed", "views"), ("addtocart", "Most added to cart", "cart_additions"), ("transaction", "Most transaction-associated", "transactions")):
            leader = max(_cache["products"].items(), key=lambda pair: pair[1][event_type], default=(None, Counter()))
            product_leaders[output_key] = {"label": label, "item_id": leader[0], "count": leader[1][event_type]}
        daily_trend = [{"date": day, "views": c["view"], "addtocart": c["addtocart"], "transactions": c["transaction"]}
                       for day, c in sorted(_cache["daily"].items())[-30:]]
        hourly_trend = [{"hour": hour, "views": c["view"], "addtocart": c["addtocart"], "transactions": c["transaction"]}
                        for hour, c in sorted(_cache["hourly"].items())[-24:]]
        session_segments = Counter()
        for session in _cache["sessions"].values():
            viewed = session["view"] > 0
            carted = session["addtocart"] > 0
            purchased = session["transaction"] > 0
            if purchased:
                session_segments["Transaction recorded"] += 1
            elif carted:
                session_segments["Cart, no transaction"] += 1
            elif viewed:
                session_segments["Views only"] += 1
        batch_trend = [{"batch_id": batch_id, "processed_events": count}
                       for batch_id, count in sorted(_cache["batches"].items())[-20:]]
    capture_is_running()  # Recover a producer that exited before saving its terminal status.
    replay = read_json(REPLAY_STATE, {"status":"not_started"})
    broker = kafka_status()
    spark_component = spark_status()
    metrics = read_json(METRICS, {})
    source_audit = source_dataset_audit()
    try:
        latest_batch = metrics.get("last_batch") or {}
        if latest_batch:
            store_spark_batch(spark_component.get("stream_started_at"), latest_batch)
        database = database_status()
        database["status"] = "error" if _database_error else "available"
        database["error"] = _database_error
        database["pending_events"] = len(_cache["database_pending_events"])
    except Exception as exc:
        database = {"status": "error", "error": str(exc),
                    "pending_events": len(_cache["database_pending_events"])}
    with _cache_lock:
        processed_ids = set(_cache["event_ids"])
        processed_details = dict(_cache["processed_event_details"])
        trace_events = list(_cache["capture_trace"])
        capture_ack_counts = _cache["capture_ack_counts"].copy()
        capture_processed_counts = _cache["capture_processed_counts"].copy()
    capture_id = replay.get("capture_id")
    current_capture_events = [event for event in trace_events if capture_id and event.get("capture_id") == capture_id]
    if not current_capture_events:
        current_capture_events = replay.get("recent_events", [])
    capture_events = []
    for event in current_capture_events:
        detail = processed_details.get(event.get("event_id"), {})
        is_processed = event.get("event_id") in processed_ids
        kafka_acked = event.get("kafka_status") == "acknowledged" or event.get("kafka_offset") is not None
        capture_events.append({**event, **detail,
            "kafka_status": "Acknowledged by Kafka" if kafka_acked else ("Sent; broker metadata unavailable" if event.get("event_id") else "Unknown"),
            "processing_status": (detail.get("preprocessing_status", "Processed by Spark") if is_processed else "Waiting for Spark")})
    acknowledged = int(capture_ack_counts.get(capture_id, 0)) if capture_id else sum(1 for event in current_capture_events if event.get("kafka_status") == "acknowledged")
    processed_capture = int(capture_processed_counts.get(capture_id, 0)) if capture_id else sum(1 for event in current_capture_events if event.get("event_id") in processed_ids)
    capture_summary = {"capture_id": capture_id, "requested": replay.get("requested", 0),
                       "kafka_acknowledged": acknowledged, "spark_processed": processed_capture,
                       "waiting_for_spark": max(0, acknowledged - processed_capture),
                       "tracked_events": len(current_capture_events)}
    return {"title": "E-Commerce Real-Time Analytics: Monitoring Click, View, and No Purchase Using Apache Kafka and Apache Spark",
        "source": "Retailrocket historical dataset", "mode": "Historical Event Replay / Real-Time Simulation",
        "tableau_view_url": os.getenv("TABLEAU_VIEW_URL", ""),
        "click": {"available": False, "count": None, "reason": "Retailrocket contains view, addtocart, and transaction events; it has no separate genuine click event."},
        "overview": {"total_events": total, "views": counts["view"], "addtocart": counts["addtocart"], "transactions": counts["transaction"],
            "unique_visitors": visitors_count, "unique_products": items_count, "eligible_sessions": eligible_count,
            "no_purchase_sessions": no_purchase_count, "no_purchase_percent": round(100*no_purchase_count/eligible_count, 2) if eligible_count else 0,
            "conversion_percent": round(100*(eligible_count-no_purchase_count)/eligible_count, 2) if eligible_count else 0},
        "funnel": [{"name":"View", "count":counts["view"]},{"name":"Add to cart", "count":counts["addtocart"]},{"name":"Transaction", "count":counts["transaction"]}],
        "events": recent[::-1], "database": database, "pipeline": {"kafka": broker,
            "replay": replay,
            "spark": spark_component}, "spark_metrics": metrics,
        "reconciliation": {"producer_acknowledged_total": int(replay.get("published", 0) or 0),
            "kafka_retained_offset_range": broker.get("retained_messages"),
            "spark_unique_processed_total": total,
            "producer_spark_match": int(replay.get("published", 0) or 0) == total,
            "capture_requested": int(replay.get("requested", 0) or 0),
            "capture_kafka_acknowledged": acknowledged,
            "capture_spark_processed": processed_capture,
            "capture_remaining": max(0, acknowledged - processed_capture)},
        "capture_events": capture_events[::-1], "capture_summary": capture_summary,
        "data_quality": {"valid_records_processed": total, "source_records": source_audit.get("source_records"),
                         "source_audit_status": source_audit.get("status"),
                         "source_missing_values": source_audit.get("missing_values", {}),
                         "source_event_types": source_audit.get("event_types", {}),
                         "invalid_records": metrics.get("invalid_records", 0),
                         "duplicates": metrics.get("duplicates", 0) + _cache["duplicate_rows"],
                         "cross_batch_duplicate_event_ids": _cache["duplicate_rows"],
                         "missing_transaction_id": source_audit.get("missing_values", {}).get("transactionid")},
        "products": top_products, "product_leaders": product_leaders,
        "daily_trend": daily_trend, "hourly_trend": hourly_trend,
        "session_segments": [{"name": name, "count": count} for name, count in session_segments.items()],
        "batch_trend": batch_trend}



def request_capture(amount, rate, resume=False):
    """Validate and start a historical Retailrocket replay producer."""
    global _capture_process
    try:
        amount = int(amount)
        rate = float(rate)
    except (TypeError, ValueError):
        return {"error": "Jumlah event dan replay rate harus berupa angka."}, 400
    if amount < 1 or amount > 100000 or not math.isfinite(rate) or rate < 0 or rate > 5000:
        return {"error": "Jumlah event harus 1-100,000 dan rate 0-5,000 event/detik."}, 400
    if not DATA.is_file():
        return {"error": f"Dataset tidak ditemukan: {DATA}"}, 503
    kafka = kafka_status()
    if kafka["status"] != "reachable":
        return {"error": "Kafka belum dapat dijangkau.", "kafka": kafka}, 503
    spark = spark_status()
    if spark.get("status") != "running":
        return {"error": "Spark harus running sebelum request data."}, 503
    capture_is_running()
    replay_state = read_json(REPLAY_STATE, {})
    if replay_state.get("status") == "running":
        return {"error": "Replay sebelumnya masih berjalan."}, 409
    if resume:
        if replay_state.get("status") != "error":
            return {"error": "Tidak ada replay gagal yang bisa dilanjutkan."}, 409
        requested_total = int(replay_state.get("requested", 0) or 0)
        capture_published = int(replay_state.get("newly_published", 0) or 0)
        remaining = max(0, requested_total - capture_published)
        if not replay_state.get("capture_id") or remaining == 0:
            return {"error": "Replay gagal tidak memiliki event yang tersisa untuk dilanjutkan."}, 409
        amount = remaining
        capture_id = str(replay_state["capture_id"])
    else:
        requested_total = amount
        capture_published = 0
        capture_id = uuid.uuid4().hex[:12]
    metrics = read_json(METRICS, {})
    # Published acknowledgements include duplicates/invalid rows; unique valid
    # output alone is not a comparable counter.
    accounted = sum(int(metrics.get(key, 0) or 0) for key in
                    ("processed_events", "invalid_records", "duplicates"))
    if int(replay_state.get("published", 0) or 0) > accounted:
        return {"error": "Spark masih memproses event Kafka. Tunggu batch selesai."}, 409
    dashboard_data()
    with _capture_lock:
        if _capture_process is not None and _capture_process.poll() is None:
            return {"error": "Replay sedang berjalan.", "replay": read_json(REPLAY_STATE, {})}, 409
        previous = int(replay_state.get("published", 0) or 0)
        limit = previous + amount
        exclude_path = ARTIFACTS / "capture-exclude-event-ids.txt"
        with _cache_lock:
            exclude_path.write_text("\n".join(sorted(_cache["event_ids"])), encoding="utf-8")
        log_path = ARTIFACTS / "capture.log"
        log_path.parent.mkdir(parents=True, exist_ok=True)
        log = log_path.open("a", encoding="utf-8")
        command = [sys.executable, str(DATA.parent.parent / "pipeline" / "replay_producer.py"),
                   "--bootstrap", f'{kafka["host"]}:{kafka["port"]}', "--topic", "retailrocket-events",
                   "--rate", str(rate), "--limit", str(limit), "--new-count", str(amount),
                   "--base-published", str(previous), "--exclude-file", str(exclude_path),
                   "--capture-id", capture_id, "--capture-published", str(capture_published),
                   "--requested-total", str(requested_total)]
        try:
            kwargs = {"cwd": str(DATA.parent.parent), "stdout": log, "stderr": subprocess.STDOUT}
            if os.name == "nt":
                kwargs["creationflags"] = subprocess.CREATE_NO_WINDOW | subprocess.CREATE_NEW_PROCESS_GROUP
            else:
                kwargs["start_new_session"] = True
            _capture_process = subprocess.Popen(command, **kwargs)
        except OSError as exc:
            log.close()
            return {"error": f"Gagal memulai replay producer: {exc}"}, 500
        log.close()
        return {"status": "started", "previous_processed": previous,
                "requested": requested_total, "remaining": amount, "target_total": limit,
                "resumed": resume, "rate_per_second": rate,
                "capture_id": capture_id, "pid": _capture_process.pid}, 202


def health_data():
    return {"web": "running", "kafka": kafka_status(),
            "spark": spark_status(),
            "replay": read_json(REPLAY_STATE, {"status": "not_started"}),
            "capture_process_running": capture_is_running()}
