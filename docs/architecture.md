# Architecture

## Project objective

**E-Commerce Real-Time Analytics: Monitoring Click, View, and No Purchase Using Apache Kafka and Apache Spark** processes historical Retailrocket events as a controlled replay. This is a real-time simulation; it is not live traffic from an operating store.

## Data flow

```text
data/events.csv (unchanged source)
  -> pipeline/replay_producer.py
  -> Kafka topic retailrocket-events
  -> pipeline/spark_processor.py (Structured Streaming)
  -> artifacts (Spark output, SQLite database, metrics, checkpoints, and component state)
  -> app.py / dashboard_backend.py (Streamlit dashboard)
```

The producer publishes source events with replay and ingestion metadata. Spark validates and transforms messages, deduplicates by event ID where applicable, calculates aggregations and writes committed output. The dashboard mirrors committed Spark events to `artifacts/retailrocket_analytics.db`; its Capture Data chart compares per-event Kafka acknowledgement timestamps with matching Spark processing timestamps. Dashboard polling is not itself evidence that Kafka or Spark is running; status must come from actual broker reachability and a fresh Spark heartbeat. Polling runs only while replay, Spark startup, or a Spark batch is active.

## Source limitations

- The source provides `view`, `addtocart`, and `transaction` event types, not a separate genuine click event.
- The supplied source does not provide product names. Product-level results should be labeled with the source `item_id`.
- A no-purchase session is an eligible session containing at least one view or add-to-cart event and no transaction event during that session. A new session begins after a gap greater than 30 minutes. It does not prove that the visitor never purchased elsewhere or later.
- Event time, ingestion time, and processing time are distinct timestamps and should be interpreted separately.

## Runtime boundaries

Kafka and Spark run in Docker Compose. The Streamlit dashboard runs with the host Python environment. The `data/` directory contains the original downloaded dataset and is not an output directory. `artifacts/` is generated runtime data and is ignored by Git; do not commit its state, logs, or checkpoints as source files.

See the root README for startup commands and the Compose deployment limitations.
