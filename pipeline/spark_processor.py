"""Validate and aggregate the Kafka stream with Spark Structured Streaming."""
import os
import time
from threading import Event, Thread
from datetime import datetime, timezone

from pyspark import StorageLevel
from pyspark.sql import SparkSession, functions as F, types as T

from common import ARTIFACTS, METRICS, SPARK_EVENTS, SPARK_HEARTBEAT, SPARK_STATE, read_json, write_json

SCHEMA = T.StructType([
    T.StructField("event_id", T.StringType()), T.StructField("event_time", T.StringType()),
    T.StructField("event_time_ms", T.LongType()), T.StructField("ingested_at", T.StringType()),
    T.StructField("visitor_id", T.StringType()), T.StructField("session_id", T.StringType()),
    T.StructField("event_type", T.StringType()), T.StructField("item_id", T.StringType()),
    T.StructField("transaction_id", T.StringType()), T.StructField("source", T.StringType()),
    T.StructField("capture_id", T.StringType())])


def main():
    broker = os.getenv("KAFKA_BOOTSTRAP", "127.0.0.1:9092")
    topic = os.getenv("KAFKA_TOPIC", "retailrocket-events")
    checkpoint = os.getenv("SPARK_CHECKPOINT", str(ARTIFACTS / "spark-checkpoint"))
    ARTIFACTS.mkdir(parents=True, exist_ok=True)
    write_json(SPARK_STATE, {"status": "starting", "startup_stage": "creating_spark_session",
                             "topic": topic, "broker": broker})
    heartbeat_stop = Event()
    def write_heartbeat():
        interval = max(2, int(os.getenv("SPARK_HEARTBEAT_INTERVAL_SECONDS", "5")))
        while not heartbeat_stop.is_set():
            write_json(SPARK_HEARTBEAT, {"heartbeat_at": datetime.now(timezone.utc).isoformat(),
                                         "pid": os.getpid(), "topic": topic})
            heartbeat_stop.wait(interval)
    heartbeat_thread = Thread(target=write_heartbeat, name="spark-health-heartbeat", daemon=True)
    heartbeat_thread.start()
    try:
        spark = (SparkSession.builder.appName("RetailrocketHistoricalEventReplay")
                 .config("spark.sql.shuffle.partitions", os.getenv("SPARK_SHUFFLE_PARTITIONS", "1"))
                 .config("spark.sql.adaptive.enabled", "false")
                 # Short local micro-batches should not pay JVM code generation
                 # and compilation costs for each complex validation plan.
                 .config("spark.sql.codegen.wholeStage", "false")
                 .config("spark.sql.codegen.factoryMode", "NO_CODEGEN")
                 # Give the local executor enough time to register its
                 # BlockManager during cold starts on Docker Desktop/WSL.
                 .config("spark.executor.heartbeatInterval", os.getenv("SPARK_EXECUTOR_HEARTBEAT_INTERVAL", "60s"))
                 .config("spark.network.timeout", os.getenv("SPARK_NETWORK_TIMEOUT", "300s"))
                 .getOrCreate())
    except Exception as exc:
        write_json(SPARK_STATE, {"status": "error", "error": str(exc), "topic": topic})
        raise
    spark.sparkContext.setLogLevel("WARN")
    write_json(SPARK_STATE, {"status": "starting", "startup_stage": "connecting_to_kafka",
                             "topic": topic, "broker": broker})
    try:
        source = (spark.readStream.format("kafka").option("kafka.bootstrap.servers", broker)
                  .option("subscribe", topic).option("startingOffsets", "earliest")
                  .option("maxOffsetsPerTrigger", int(os.getenv("SPARK_MAX_OFFSETS_PER_TRIGGER", "500")))
                  .load())
    except Exception as exc:
        write_json(SPARK_STATE, {"status": "error", "startup_stage": "connecting_to_kafka",
                                 "error": str(exc), "topic": topic, "broker": broker})
        raise
    write_json(SPARK_STATE, {"status": "starting", "startup_stage": "kafka_source_ready",
                             "topic": topic, "broker": broker})
    events = (source.select(F.from_json(F.col("value").cast("string"), SCHEMA).alias("e"),
                            F.col("partition").alias("kafka_partition"),
                            F.col("offset").alias("kafka_offset"),
                            F.col("timestamp").alias("kafka_received_at"))
              .select("e.*", "kafka_partition", "kafka_offset", "kafka_received_at")
              .withColumn("parsed_event_time", F.to_timestamp("event_time"))
              .withColumn("processed_at", F.current_timestamp()))

    def process_batch(batch, batch_id):
        started = time.perf_counter()
        previous_state = read_json(SPARK_STATE, {})
        write_json(SPARK_STATE, {**previous_state, "status": "running", "active_batch_id": int(batch_id),
                          "active_batch_status": "reading and validating", "active_batch_started_at": datetime.now(timezone.utc).isoformat(),
                          "updated_at": datetime.now(timezone.utc).isoformat(), "topic": topic})
        valid_expr = (F.col("event_id").isNotNull() & F.col("event_time_ms").isNotNull() &
                      (F.col("event_time_ms") > 0) & F.col("parsed_event_time").isNotNull() &
                      F.col("visitor_id").isNotNull() & F.col("session_id").isNotNull() &
                      F.col("item_id").isNotNull() & F.col("event_type").isin("view", "addtocart", "transaction"))
        batch.persist(StorageLevel.MEMORY_AND_DISK)
        valid = None
        try:
            # One pass calculates raw-valid and total counts. Persisting the Kafka
            # microbatch avoids rereading it for later validation and aggregation.
            raw = batch.agg(F.count("*").alias("total"),
                            F.sum(F.when(valid_expr, 1).otherwise(0)).alias("raw_valid")).first().asDict()
            raw_count = int(raw.get("total", 0) or 0)
            raw_valid_count = int(raw.get("raw_valid", 0) or 0)
            state = read_json(SPARK_STATE, {})
            write_json(SPARK_STATE, {**state, "active_batch_status": "deduplicating and aggregating",
                          "active_batch_received": raw_count, "active_batch_schema_valid": raw_valid_count,
                          "updated_at": datetime.now(timezone.utc).isoformat()})
            valid = batch.filter(valid_expr).dropDuplicates(["event_id"]).persist(StorageLevel.MEMORY_AND_DISK)
            # This is the only aggregate pass over the validated, deduplicated data.
            agg = valid.agg(
                F.count("*").alias("processed_events"),
                F.sum(F.when(F.col("event_type") == "view", 1).otherwise(0)).alias("views"),
                F.sum(F.when(F.col("event_type") == "addtocart", 1).otherwise(0)).alias("addtocart"),
                F.sum(F.when(F.col("event_type") == "transaction", 1).otherwise(0)).alias("transactions")
            ).first().asDict()
            raw_valid = int(raw.get("raw_valid", 0) or 0)
            invalid_count = int(raw.get("total", 0) or 0) - raw_valid
            duplicates = raw_valid - int(agg.get("processed_events", 0) or 0)
            # Spark writes validated rows as durable JSON parts, consumed by API.
            state = read_json(SPARK_STATE, {})
            write_json(SPARK_STATE, {**state, "active_batch_status": "writing processed output",
                          "active_batch_duplicates": duplicates, "active_batch_invalid": invalid_count,
                          "active_batch_output": int(agg.get("processed_events", 0) or 0),
                          "updated_at": datetime.now(timezone.utc).isoformat()})
            if int(agg.get("processed_events", 0) or 0):
                (valid.withColumn("spark_batch_id", F.lit(int(batch_id)))
                      .withColumn("preprocessing_status", F.lit("valid"))
                      .coalesce(1).write.mode("append").json(str(SPARK_EVENTS)))
            old = read_json(METRICS, {})
            for key in ("processed_events", "views", "addtocart", "transactions"):
                old[key] = int(old.get(key, 0) or 0) + int(agg.get(key, 0) or 0)
            old["invalid_records"] = int(old.get("invalid_records", 0)) + invalid_count
            old["duplicates"] = int(old.get("duplicates", 0)) + duplicates
            elapsed = round(time.perf_counter() - started, 3)
            old["last_batch"] = {"id": int(batch_id), "received": int(raw.get("total", 0) or 0),
                                  "schema_valid": raw_valid, "valid": int(agg.get("processed_events", 0) or 0),
                                  "invalid": invalid_count, "duplicates": duplicates,
                                  "processing_seconds": elapsed,
                                  "at": datetime.now(timezone.utc).isoformat()}
            old["last_successful_execution"] = datetime.now(timezone.utc).isoformat()
            write_json(METRICS, old)
            write_json(SPARK_STATE, {"status": "running", "last_batch_id": int(batch_id),
                               "last_batch_status": "completed", "last_batch_received": int(raw.get("total", 0) or 0),
                               "last_batch_valid": int(agg.get("processed_events", 0) or 0),
                               "last_batch_invalid": invalid_count, "last_batch_duplicates": duplicates,
                               "last_batch_seconds": elapsed, "active_batch_id": None, "active_batch_status": "idle",
                               "last_successful_execution": old["last_successful_execution"], "topic": topic})
        finally:
            if valid is not None:
                valid.unpersist()
            batch.unpersist()

    try:
        query = (events.writeStream.foreachBatch(process_batch).option("checkpointLocation", checkpoint)
                 .trigger(processingTime=os.getenv("SPARK_TRIGGER", "2 seconds")).start())
        if query.isActive:
            previous_metrics = read_json(METRICS, {})
            previous_batch = previous_metrics.get("last_batch") or {}
            write_json(SPARK_STATE, {**read_json(SPARK_STATE, {}), "status": "running", "topic": topic,
                               "active_batch_id": None, "active_batch_status": "idle",
                               "stream_started_at": datetime.now(timezone.utc).isoformat(),
                               "last_batch_id": previous_batch.get("id"),
                               "last_batch_status": "completed" if previous_batch else None,
                               "last_batch_received": previous_batch.get("received"),
                               "last_batch_valid": previous_batch.get("valid"),
                               "last_batch_invalid": previous_batch.get("invalid"),
                               "last_batch_duplicates": previous_batch.get("duplicates"),
                               "last_batch_seconds": previous_batch.get("processing_seconds"),
                               "last_successful_execution": previous_metrics.get("last_successful_execution")})
        query.awaitTermination()
    except Exception as exc:
        write_json(SPARK_STATE, {"status": "error", "error": str(exc), "topic": topic,
                                 "last_successful_execution": read_json(METRICS, {}).get("last_successful_execution")})
        raise
    finally:
        heartbeat_stop.set()
        heartbeat_thread.join(timeout=2)
        if not read_json(SPARK_STATE, {}).get("status") == "error":
            write_json(SPARK_STATE, {"status": "stopped", "last_successful_execution":
                              read_json(METRICS, {}).get("last_successful_execution")})
        SPARK_HEARTBEAT.unlink(missing_ok=True)
        spark.stop()


if __name__ == "__main__":
    main()
