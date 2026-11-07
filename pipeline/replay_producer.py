"""Replay source rows, ordered by event time, into Kafka."""
import argparse
import csv
from itertools import islice
import json
import os
import time
import uuid
from collections import deque
from datetime import datetime, timezone

from kafka import KafkaProducer
from common import ARTIFACTS, DATA, METRICS, REPLAY_STATE, read_json, write_json


def rows(path, limit=None):
    # Retailrocket contains many out-of-order adjacent timestamps, so sort the
    # selected records before assigning sessions and publishing them.
    with open(path, newline="", encoding="utf-8") as f:
        source_rows = enumerate(csv.DictReader(f), start=1)
        # A limited demo only needs to read its source prefix before sorting.
        # This avoids loading all 2.75M source rows just to publish a small sample.
        data = list(islice(source_rows, limit)) if limit is not None else list(source_rows)
    data.sort(key=lambda pair: int(pair[1]["timestamp"]))
    return data


def make_events(records, session_gap_minutes):
    last_seen, session_seq = {}, {}
    gap_ms = session_gap_minutes * 60_000
    for source_row_id, row in records:
        visitor, ts = row["visitorid"], int(row["timestamp"])
        if visitor not in last_seen or ts - last_seen[visitor] > gap_ms:
            session_seq[visitor] = session_seq.get(visitor, 0) + 1
        last_seen[visitor] = ts
        event = row["event"]
        yield {
            "event_id": f"retailrocket:{source_row_id}",
            "event_time": datetime.fromtimestamp(ts / 1000, timezone.utc).isoformat(),
            "event_time_ms": ts,
            "ingested_at": datetime.now(timezone.utc).isoformat(),
            "visitor_id": visitor,
            "session_id": f"{visitor}:{session_seq[visitor]}",
            "event_type": event,
            "item_id": row["itemid"],
            "transaction_id": row.get("transactionid") or None,
            "source": "retailrocket_historical_replay",
        }


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--bootstrap", default=os.getenv("KAFKA_BOOTSTRAP", "127.0.0.1:9092"))
    parser.add_argument("--topic", default=os.getenv("KAFKA_TOPIC", "retailrocket-events"))
    parser.add_argument("--rate", type=float, default=100, help="Events per second; 0 means no delay")
    parser.add_argument("--limit", type=int, help="Replay a prefix for a quick demo")
    parser.add_argument("--skip", type=int, default=0,
                        help="Skip this many ordered events from the selected sample when resuming")
    parser.add_argument("--exclude-file", help="Text file with event IDs already captured")
    parser.add_argument("--new-count", type=int, help="Publish this many previously unseen events")
    parser.add_argument("--base-published", type=int, default=0,
                        help="Previously published count to retain in replay status")
    parser.add_argument("--capture-id", default=None, help="Dashboard request identifier for event tracing")
    parser.add_argument("--capture-published", type=int, default=0,
                        help="Previously acknowledged events in this capture")
    parser.add_argument("--requested-total", type=int,
                        help="Total requested events across resumed attempts")
    parser.add_argument("--loop", action="store_true", help="Restart the historical replay after EOF")
    parser.add_argument("--session-gap-minutes", type=int, default=30)
    args = parser.parse_args()
    if (args.rate < 0 or args.session_gap_minutes <= 0 or args.skip < 0 or
            args.base_published < 0 or args.capture_published < 0):
        parser.error("rate and skip must be nonnegative; session gap must be positive")
    if args.requested_total is not None and args.requested_total <= 0:
        parser.error("requested-total must be positive")
    if args.new_count is not None and args.new_count <= 0:
        parser.error("new-count must be positive")
    if args.new_count is not None and args.loop:
        parser.error("new-count cannot be combined with loop")
    if args.limit is not None and args.skip >= args.limit:
        parser.error("skip must be smaller than limit")
    producer = None
    last_connect_error = None
    # Kafka can accept TCP connections before its metadata API is ready.
    # Retry the protocol handshake briefly before failing the capture.
    for attempt in range(3):
        try:
            producer = KafkaProducer(bootstrap_servers=args.bootstrap,
                value_serializer=lambda value: json.dumps(value).encode("utf-8"),
                key_serializer=lambda value: value.encode("utf-8"), acks="all",
                enable_idempotence=True, max_in_flight_requests_per_connection=1,
                linger_ms=5, batch_size=65536, max_block_ms=60000,
                request_timeout_ms=30000, retry_backoff_ms=1000,
                delivery_timeout_ms=180000, api_version_auto_timeout_ms=5000)
            break
        except Exception as exc:
            last_connect_error = exc
            if attempt < 2:
                time.sleep(1)
    if producer is None:
        exc = last_connect_error or RuntimeError("Kafka producer could not be initialized")
        # A broker may accept TCP connections before its Kafka metadata API is
        # ready. Preserve the previous published offset so the dashboard can
        # safely retry after the broker becomes healthy instead of appearing
        # permanently out of sync with Spark.
        previous = read_json(REPLAY_STATE, {})
        published = previous.get("published")
        if published is None:
            published = read_json(METRICS, {}).get("processed_events", 0)
        write_json(REPLAY_STATE, {**previous, "status": "error",
                                  "published": int(published or 0), "error": str(exc)})
        raise SystemExit(f"Kafka unavailable at {args.bootstrap}: {exc}")

    sent = args.base_published + args.skip
    newly_sent = 0
    capture_sent = args.capture_published
    requested_total = args.requested_total or args.new_count
    final_status = "completed"
    final_error = None
    recent_events = deque(maxlen=50)
    last_state_write = 0.0
    capture_id = args.capture_id or uuid.uuid4().hex[:12]
    trace_path = ARTIFACTS / "capture-trace.jsonl"
    trace_path.parent.mkdir(parents=True, exist_ok=True)
    try:
        write_json(REPLAY_STATE, {"status": "running", "published": sent,
            "topic": args.topic, "source": str(DATA.name), "rate_per_second": args.rate,
            "requested": requested_total, "newly_published": capture_sent,
            "attempt_published": newly_sent, "capture_id": capture_id, "recent_events": []})
        while True:
            excluded = set()
            if args.exclude_file:
                with open(args.exclude_file, encoding="utf-8") as existing:
                    excluded = {line.strip() for line in existing if line.strip()}
            if args.new_count is not None:
                prefix_limit = args.limit or args.base_published + args.new_count
                while True:
                    records = rows(DATA, prefix_limit)
                    selected = list(islice((event for event in make_events(records, args.session_gap_minutes)
                                            if event["event_id"] not in excluded), args.new_count))
                    if len(selected) >= args.new_count or len(records) < prefix_limit:
                        break
                    prefix_limit = max(prefix_limit * 2, prefix_limit + args.new_count)
                pending_events = selected
            else:
                ordered_events = make_events(rows(DATA, args.limit), args.session_gap_minutes)
                pending_events = islice(ordered_events, args.skip, None)
            with trace_path.open("a", encoding="utf-8") as trace:
                pending = deque()

                def record_ack(entry):
                    nonlocal sent, newly_sent, capture_sent, last_state_write
                    event, future, receipt = entry
                    metadata = future.get(timeout=185)
                    acknowledged_at = receipt.get("at", datetime.now(timezone.utc).isoformat())
                    traced_event = {**event, "kafka_status": "acknowledged",
                                    "kafka_partition": int(metadata.partition), "kafka_offset": int(metadata.offset),
                                    "kafka_acknowledged_at": acknowledged_at}
                    trace.write(json.dumps(traced_event, separators=(",", ":")) + "\n")
                    sent += 1
                    newly_sent += 1
                    capture_sent += 1
                    recent_events.append(traced_event)
                    now = time.monotonic()
                    # Keep actual broker acknowledgements and source payloads
                    # available for the dashboard's per-event trace.
                    if newly_sent == 1 or now - last_state_write >= 0.5:
                        trace.flush()
                        write_json(REPLAY_STATE, {"status": "running", "published": sent,
                            "topic": args.topic, "source": str(DATA.name), "rate_per_second": args.rate,
                            "requested": requested_total, "capture_id": capture_id,
                            "newly_published": capture_sent, "attempt_published": newly_sent,
                            "last_published_at": acknowledged_at,
                            "recent_events": list(recent_events)})
                        last_state_write = now
                next_send = time.monotonic()
                for event in pending_events:
                    if args.rate:
                        time.sleep(max(0, next_send - time.monotonic()))
                    event["capture_id"] = capture_id
                    event["ingested_at"] = datetime.now(timezone.utc).isoformat()
                    receipt = {}
                    future = producer.send(args.topic, key=event["event_id"], value=event)
                    future.add_callback(lambda metadata, receipt=receipt: receipt.update(
                        at=datetime.now(timezone.utc).isoformat()))
                    pending.append((event, future, receipt))
                    if args.rate:
                        next_send = max(next_send + 1 / args.rate, time.monotonic())
                    # Bounded asynchronous sends let Kafka batch messages.
                    while pending and (pending[0][1].is_done or len(pending) >= 256):
                        record_ack(pending.popleft())
                while pending:
                    record_ack(pending.popleft())
                trace.flush()
            producer.flush(timeout=15)
            if args.new_count is not None and newly_sent < args.new_count:
                print(f"Dataset exhausted: {newly_sent} new events published (requested {args.new_count})", flush=True)
            else:
                print(f"Replay complete: {sent} total messages published to {args.topic}", flush=True)
            if not args.loop:
                break
    except KeyboardInterrupt:
        final_status = "stopped"
        print(f"Replay stopped after {sent} messages", flush=True)
    except Exception as exc:
        final_status = "error"
        final_error = str(exc)
        raise
    finally:
        # Save terminal status before a potentially slow producer shutdown.
        final_state = {"status": final_status, "published": sent, "topic": args.topic,
                       "capture_id": capture_id, "recent_events": list(recent_events),
                       "rate_per_second": args.rate, "newly_published": capture_sent,
                       "attempt_published": newly_sent,
                       "last_published_at": datetime.now(timezone.utc).isoformat()}
        if requested_total is not None:
            final_state["requested"] = requested_total
        if final_error:
            final_state["error"] = final_error
        if final_status == "completed":
            final_state["completed_at"] = datetime.now(timezone.utc).isoformat()
        write_json(REPLAY_STATE, final_state)
        try:
            producer.close(timeout=5)
        except Exception as close_exc:
            print(f"Kafka producer shutdown failed: {close_exc}", file=sys.stderr, flush=True)
            if final_status == "completed":
                final_state.update({"status": "error", "error": f"Kafka producer shutdown failed: {close_exc}"})
                write_json(REPLAY_STATE, final_state)


if __name__ == "__main__":
    main()
