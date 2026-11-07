# Data dictionary and metric rules

## Retailrocket source files

| File | Purpose |
| --- | --- |
| `events.csv` | Historical visitor events: timestamp, visitor, event type, item, and transaction ID where present. |
| `item_properties_part1.csv`, `item_properties_part2.csv` | Historical item properties, when needed for later enrichment. They do not provide verified human-readable product titles. |
| `category_tree.csv` | Item category hierarchy. |

The dataset is retained as downloaded. Preprocessing and replay outputs belong under `artifacts/`.

## Event types

| Event | Meaning in this project |
| --- | --- |
| `view` | One source product-view event. Repeated views remain repeated events. |
| `addtocart` | One source add-to-cart event. |
| `transaction` | One source transaction event. |
| Click | Unavailable in this dataset; product views are not relabeled as clicks. |

Counts of events, distinct visitors, distinct products, and sessions are different units and must be labeled separately.

## Session and no-purchase rules

Events are ordered by event timestamp for each visitor. A session is split when the gap between consecutive events is greater than 30 minutes. A session is eligible if it includes at least one view or add-to-cart event. An eligible session is labeled “without recorded purchase” if it contains no transaction event. This rule describes the observed session only.

## Pipeline timestamps

- **Event time:** timestamp recorded in the historical source.
- **Ingestion time:** time the replay/pipeline received or published the event.
- **Processing time:** time Spark processed the event or micro-batch.

## Quality measurements

Source-record counts and missing-value counts describe the original CSV. Invalid, duplicate, published, retained-in-Kafka, and Spark-processed counts describe separate pipeline stages; they are not interchangeable. Missing transaction IDs on non-transaction rows are expected and should not automatically be treated as invalid records.
