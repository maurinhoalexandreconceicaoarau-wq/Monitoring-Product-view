"""Shared paths and simple atomic JSON helpers for the Retailrocket demo."""
import json
import os
from pathlib import Path
import time

ROOT = Path(__file__).resolve().parents[1]
DATA = ROOT / "data" / "events.csv"
ARTIFACTS = Path(os.environ.get("ARTIFACTS_DIR", ROOT / "artifacts"))
STATE = ARTIFACTS / "state.json"
REPLAY_STATE = ARTIFACTS / "replay.json"
SPARK_STATE = ARTIFACTS / "spark.json"
SPARK_HEARTBEAT = ARTIFACTS / "spark-heartbeat.json"
EVENT_LOG = ARTIFACTS / "processed_events.jsonl"
METRICS = ARTIFACTS / "metrics.json"
SPARK_EVENTS = ARTIFACTS / "spark-events"


def read_json(path, default=None):
    try:
        return json.loads(Path(path).read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return default if default is not None else {}


def write_json(path, value):
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    temp = path.with_suffix(path.suffix + ".tmp")
    temp.write_text(json.dumps(value, separators=(",", ":")), encoding="utf-8")
    # Windows file scanners and bind-mounted Docker readers can briefly hold a
    # shared output file open. Retry transient replace failures before surfacing
    # a real write error to the component status.
    for attempt in range(6):
        try:
            temp.replace(path)
            return
        except PermissionError:
            if attempt == 5:
                raise
            time.sleep(0.05 * (attempt + 1))


def update_json(path, key, value):
    data = read_json(path, {})
    data[key] = value
    write_json(path, data)
