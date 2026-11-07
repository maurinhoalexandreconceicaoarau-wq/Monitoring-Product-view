# E-Commerce Real-Time Analytics: Monitoring Click, View, and No Purchase Using Apache Kafka and Apache Spark

This project replays the downloaded Retailrocket historical event dataset through Apache Kafka, processes it with Spark Structured Streaming, and presents processed results in a Streamlit dashboard. The mode is **Historical Event Replay / Real-Time Simulation**, not live traffic from an operating store.

## Architecture

```text
Retailrocket historical CSV
  -> configurable replay producer
  -> Kafka topic: retailrocket-events
  -> Spark Structured Streaming validation and aggregation
  -> committed Spark JSON output and metrics
  -> SQLite event store (idempotent by event_id)
  -> Streamlit dashboard (refreshes while replay or Spark processing is active)
```

The original dataset files remain unchanged. Spark output, runtime state, and the local SQLite database are stored under `artifacts/`.

Project structure and operating notes are in [`docs/`](docs/). Windows PowerShell helpers are in [`scripts/`](scripts/).

## Requirements

- Docker Desktop with its engine running
- The downloaded Retailrocket files in `data/`

## Run the full system with Docker Desktop

Open Docker Desktop and wait until its engine is running. In PowerShell, go to the project folder and build/start the complete Compose application:

```powershell
docker compose up -d --build
```

The first build downloads the dashboard image and Python packages; Spark also downloads its Kafka connector on startup. Wait until all services are healthy:

```powershell
docker compose ps
```

Open `http://127.0.0.1:8501`. In Docker Desktop, open **Containers** and find the `bigdata_ecommerce_project` Compose application. Use the application controls to start or stop its services; Compose waits for Kafka, creates the `retailrocket-events` topic, starts Spark, and then starts the dashboard.

The application is also available from PowerShell:

```powershell
docker compose stop
docker compose start
docker compose logs -f
```

`stop` preserves the containers for restarting in Docker Desktop. Use `docker compose down` only when you want to remove the containers and network; it does not delete the project files, source dataset, bind-mounted `artifacts/` data, or named Kafka/Spark volumes. Do not use `docker compose down --volumes` unless you intentionally want to erase Kafka's stored messages and Spark's checkpoint. `./scripts/start.ps1`, `./scripts/status.ps1`, and `./scripts/stop.ps1` are Windows PowerShell shortcuts for starting, checking, and stopping the full Docker application.

The dashboard is bound to `127.0.0.1` and is not exposed to other machines on the network. The original dataset is mounted read-only, while generated output and state remain in the project `artifacts/` folder.

## Publish a lightweight online preview

The root-level `streamlit_cloud_app.py` is a portfolio preview for Streamlit Community Cloud. It uses generated sample events and does not start Kafka or Spark, so it can run from this GitHub repository without uploading the large Retailrocket CSV files. It clearly labels all displayed values as generated examples. It is separate from `app.py`, which remains the full local Kafka/Spark dashboard.

To publish the preview:

1. Sign in at [Streamlit Community Cloud](https://share.streamlit.io/) with the GitHub account that can access this repository.
2. Choose **Create app**, then select this repository, branch `main`, and entrypoint `streamlit_cloud_app.py`.
3. Deploy. The service will install the lightweight packages from the root `requirements.txt` and provide a public `*.streamlit.app` URL.

Push later code changes to `main`; Community Cloud can redeploy from the updated GitHub source. The preview is not connected to the source dataset or live pipeline. Use Docker Compose locally for the full Kafka and Spark simulation.

## Dashboard menus

- **Dashboard:** processed event totals, daily historical event trend, funnel, click data limitation, product IDs, no-purchase sessions, and pipeline state.
- **Capture Data:** request a configurable slice of Retailrocket records and see actual payloads, Kafka publication, Spark processing status, and a cumulative live chart based on per-event acknowledgement and processing timestamps.
- **Event Stream:** inspect recent Spark-processed events and their event, ingestion, and processing timestamps.
- **Analytics:** product event aggregations and data-quality counts.
- **Pipeline:** Kafka, Spark, replay, message counts, and last successful Spark execution.
- **Database:** inspect and filter persisted Spark events, view saved batch summaries, and export queried rows to CSV.
- **Visualization:** event trends, event mix, product comparisons, funnel, session patterns, Spark batch sizes, and replay data quality.
- **Tableau Exports:** download Spark-processed event data as compressed CSV and current metrics as CSV for Tableau Desktop.

## Data definitions and limitations

- Retailrocket has original `view`, `addtocart`, and `transaction` event types. It does not provide a separate genuine click event, so the dashboard marks click monitoring unavailable.
- Product titles are not present in the supplied files. Product analytics identify items by source `item_id`; no product names are invented.
- View, cart, and transaction counts are event counts, not unique people. Transaction-associated item counts follow the `item_id` on transaction events.
- A visitor session ends when the next event arrives after a gap greater than 30 minutes. An eligible session has at least one view or add-to-cart. It is classified as no-purchase when no transaction event is recorded in that session. This does not mean the visitor never purchased elsewhere or later.
- The dashboard refreshes every three seconds while a replay, Spark startup, or Spark batch is active, then stops polling when the pipeline is idle. This is only a UI polling interval, not a processing-time guarantee. Kafka and Spark indicators come from runtime reachability and actual component state, not configuration alone.
- Spark committed event rows are mirrored into `artifacts/retailrocket_analytics.db` (SQLite). The `processed_events` table keeps event, ingestion, and processing times with Kafka offsets and Spark batch IDs; `spark_batches` stores batch summaries. `event_id` is the primary key, so retries do not create duplicate database rows. The SQLite file is generated output and is excluded from version control. The source CSV is not modified.
- Source CSV row counts, missing values, and event-type totals are generated by a background audit and cached with the source file size and modification timestamp. The original source file is read only.
- Spark emits a separate five-second heartbeat. The dashboard marks Spark stale if that heartbeat is older than 20 seconds; a saved `running` state by itself is not proof of a live processor.
- Spark reads at most 500 Kafka offsets per trigger by default (`SPARK_MAX_OFFSETS_PER_TRIGGER` in `docker-compose.yml`). This rate limit can be tuned for the demo machine.
- Pipeline counts use distinct units: producer acknowledgements are cumulative sends confirmed by Kafka; Kafka retained records are estimated from partition end offsets minus beginning offsets and may include duplicates or reflect retention; Spark processed totals are unique event IDs in committed output. These numbers are displayed separately and should not be assumed interchangeable.

## Local demo scope and production gaps

The Compose setup is a single-node teaching/demo deployment: one Kafka broker, one partition, plaintext local connections, Spark local mode with one worker, a local checkpoint/output directory, and a dashboard bound to loopback. It demonstrates the processing flow but does not provide broker redundancy, authentication, remote durable storage, or high availability. Do not expose these local services to an untrusted network. A production deployment would need a multi-broker Kafka cluster with replication, secured listeners and credentials, durable shared checkpoint/output storage, authentication for the dashboard, and operational alerting.

Before presenting results, capture one replay and compare its requested count, Kafka acknowledgements, Spark processed count, invalid/duplicate totals, and the event-by-event trace. Keep the replay rate within the laptop's capacity; the Spark offset cap limits individual micro-batches, not the overall replay size.

## Evidence

Use the dashboard event stream and pipeline pages alongside `artifacts/replay.json`, `artifacts/spark.json`, `artifacts/metrics.json`, producer logs, Spark logs, and Kafka topic offsets to document a real replay. A stopped service must remain visibly stopped; historical snapshots are not presented as live processing.

See [`docs/demo_guide.md`](docs/demo_guide.md) for a repeatable presentation checklist and [`docs/architecture.md`](docs/architecture.md) for component responsibilities.
