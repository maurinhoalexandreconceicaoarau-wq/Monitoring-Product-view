# Demo and evidence guide

## Start

1. Open Docker Desktop and wait for its engine to become available.
2. In PowerShell, change to the project directory and run `./scripts/start.ps1`, or follow the README commands manually.
3. Open `http://127.0.0.1:8501` and inspect the Pipeline page. Confirm Kafka is reachable and Spark reports a fresh running heartbeat before requesting data.

## Capture a replay

1. Open **Capture Data** and select a manageable event count and replay rate. Start with the default rate, especially when Spark is still processing earlier batches.
2. Request the data once. Inspect actual event payloads and their IDs/timestamps in the event stream.
3. Watch the **Live event arrival & processing** chart. Its Kafka line uses broker acknowledgement timestamps; its Spark line uses timestamps for matching event IDs found in committed Spark output. The chart covers the most recent 2,000 traced events; the counters above it cover the full request.
4. Compare the request count with producer acknowledgements, Kafka topic offsets, and Spark processed/invalid/duplicate counts. Allow Spark to catch up before starting another capture.
5. Capture screenshots of the chart, event stream, and Pipeline page while the run is in progress and after it completes.

The replay producer uses Kafka idempotence and allows retriable broker errors, including temporary partition-leader changes, up to the configured delivery timeout. If a replay still ends with an error after partial Kafka acknowledgements, wait for Spark to process the acknowledged records, then use **Resume remaining … events** on the Capture Data page. It continues the same request and capture trace rather than counting a new request.

## Explain results accurately

- Call this **Historical Event Replay / Real-Time Simulation**, not live store traffic.
- Explain that click events and human-readable product names are absent from the supplied source.
- Explain that “without recorded purchase” applies only to the defined session and observed data.
- Distinguish event totals from unique visitors, products, and sessions.
- Use source event time separately from ingestion and processing time.
- The dashboard polls every three seconds only while replay, Spark startup, or a Spark batch is active; polling stops when the pipeline becomes idle.
- Show the actual component status. A saved historical result does not prove a service is currently running.

## Stop

Press `Ctrl+C` in the dashboard terminal, then run `./scripts/stop.ps1` to stop Kafka and Spark containers.

## Evidence locations

Runtime evidence is written under the ignored `artifacts/` directory. Key state files include replay status, Spark status/heartbeat, metrics, committed Spark output, and the capture trace. Keep the original `data/` files unchanged.
