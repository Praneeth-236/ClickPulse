# ClickPulse — Data Contract

**Project:** ClickPulse: Build Your Own Analytics System
**Course:** Big Data 2026 (UE24CS343AB2)
**Version:** 1.0 — Initial team contract
**Team:** Praneeth, Tejas, Shreyas, Gurudath
**Status:** Proposed baseline for team approval

---

## 1. Purpose

This document defines the common data contract for ClickPulse.

All four team members must implement their components against this contract so that Kafka, HDFS, Spark, Iceberg, Druid and Airflow work together without incompatible assumptions.

This document defines:

- Input schema and data types
- Valid event types
- Data validation rules
- Raw-data preservation
- Deduplication rules
- Incremental upsert rules
- Rejection and drop handling
- HDFS and Iceberg conventions
- Druid synchronization requirements
- Airflow execution interfaces
- Expected dataset results
- Acceptance tests

### Source-of-truth hierarchy

1. Official ClickPulse problem statement.
2. Actual `clicks_day1.jsonl` and `clicks_day2.jsonl` datasets.
3. This team contract for implementation conventions.

If this document conflicts with the official problem statement, the official specification takes priority.

---

## 2. Technology decisions

| Component | Selected technology |
|---|---|
| Message broker | Apache Kafka |
| Kafka coordination | KRaft |
| Raw storage | HDFS |
| Processing | Apache Spark (PySpark) |
| Curated table | Apache Iceberg |
| Analytics serving | Apache Druid |
| Orchestration | Apache Airflow |
| Dashboard | Streamlit |
| Programming language | Python / PySpark |

**Iceberg is our selected table format.** We will not implement both Iceberg and Hudi.

Software versions and compatibility must be frozen separately after testing the Spark, Hadoop and Iceberg combination.

---

## 3. Distributed architecture

| VM | Owner | Official responsibility |
|---|---|---|
| VM1 | Gurudath | Producer, Kafka Broker 1 with KRaft controller, Broker 2 |
| VM2 | Tejas | Airflow DAG, Kafka consumer, HDFS NameNode and DataNode |
| VM3 | Praneeth | Spark Master, Worker and Driver in standalone mode |
| VM4 | Shreyas | Druid and dashboard |

### Architecture rules

1. Both Kafka brokers must remain running throughout the project.
2. Airflow orchestrates producer, consumer, Spark and Druid ingestion jobs.
3. Airflow must not start or stop Kafka brokers for individual runs.
4. VM1, VM2 and VM3 connect to the same HDFS cluster.
5. VM2 hosts the HDFS NameNode and DataNode.
6. Spark reads from HDFS or the curated table, not directly from Kafka.
7. Structured Streaming is not required.
8. Druid serves the current curated state for analytics.

---

## 4. Canonical event schema

Both uploaded datasets contain the following 11 fields.

| Field | Logical type | Nullability | Description |
|---|---|---|---|
| `event_id` | STRING | Required | Unique event identifier |
| `user_id` | STRING | Required | User identifier |
| `session_id` | STRING | Required | Session identifier |
| `event_type` | STRING | Required | Activity type |
| `product_id` | STRING | Nullable | Product identifier |
| `category` | STRING | Nullable | Product category |
| `price` | DECIMAL | Nullable | Product price |
| `quantity` | INTEGER | Nullable | Product quantity |
| `device` | STRING | Observed non-null | Device used |
| `event_time` | TIMESTAMP | Required | Event occurrence time |
| `updated_at` | TIMESTAMP | Required | Record update timestamp |

### Five valid event types

Only these five values are valid:

1. `page_view`
2. `product_view`
3. `add_to_cart`
4. `remove_from_cart`
5. `purchase`

Any other value must be rejected.

### Observed device values

- `mobile`
- `desktop`
- `tablet`

### Observed product categories

- Electronics
- Sports
- Beauty
- Books
- Fashion
- Grocery
- Home & Kitchen

These are observed values, not an exhaustive validation whitelist.

### Null handling

The dataset contains legitimate null values for:

- `product_id`
- `category`
- `price`
- `quantity`

These fields must not be made universally mandatory.

For example, a `page_view` event can have null `product_id`, `price` and `quantity`.

The specification does not define a rule that rejects missing `device`, `category`, or `product_id`. Do not invent such rejection rules.

### Timestamp convention

All valid timestamps in the supplied datasets use UTC notation with `Z`.

The curated `event_date` must be calculated from `event_time` in UTC.

---

## 5. Raw-data preservation

**Requirement: Every original input line must land unchanged in HDFS.**

This includes:

- Valid JSON records
- Invalid JSON objects
- Malformed or unreadable lines
- Identical duplicate lines
- Conflicting versions

### Raw-data rules

1. Do not parse or clean the input before raw landing.
2. Do not remove duplicates during Kafka production or consumption.
3. Do not discard malformed JSON.
4. Do not serialize a parsed JSON object again and claim it is the original line.
5. Preserve every original line occurrence.

Recommended approach:

- Producer reads each original JSONL line.
- Kafka message value contains the original line bytes.
- Source-file and line-number metadata are carried separately, for example through Kafka headers.
- Consumer writes the original line to HDFS without JSON reserialization.

### Proposed HDFS structure

```text
/clickpulse/
│
├── raw/
│   ├── day1/
│   └── day2/
│
├── rejected/
│   ├── day1/
│   └── day2/
│
├── exports/
│   └── current-state/
│
└── metadata/
    ├── manifests/
    └── processing-ledger/
```

The directory structure is a team convention, not an official mandated path.

Raw landing must be recoverable and must not overwrite an already committed source batch.

---

## 6. Validation and rejection rules

A record is invalid if any of the following conditions apply.

| ID | Validation condition | Rejection reason |
|---|---|---|
| V01 | Not a readable JSON object | `unreadable_or_not_object` |
| V02 | Missing, null or empty `event_id` | `missing_or_empty_event_id` |
| V03 | Missing, null or empty `user_id` | `missing_or_empty_user_id` |
| V04 | Missing, null or empty `session_id` | `missing_or_empty_session_id` |
| V05 | `event_type` outside the five valid values | `invalid_event_type` |
| V06 | `event_time` cannot be parsed | `invalid_event_time` |
| V07 | Negative `price` | `negative_price` |
| V08 | Negative `quantity` | `negative_quantity` |

### Important validation details

- Zero price is valid.
- Zero quantity is valid.
- Null price and quantity must be preserved.
- Null product ID and category must be preserved.
- An invalid event must never enter the curated table or Druid.
- A record with multiple validation failures should retain all applicable rejection reasons.
- Every invalid occurrence counts separately, even when its raw line is repeated.

Whitespace-only identifier strings should be treated as empty as an implementation convention.

For unexpected nonnumeric price or quantity values, do not silently convert them to null. Record a schema/type error and resolve the precise rejection policy before introducing such inputs.

### Rejected-record schema

The separate rejected-record table must contain at least:

| Field | Purpose |
|---|---|
| `raw_line` | Original input line |
| `source_file` | Original dataset name |
| `source_line_number` | Original line position |
| `rejection_reasons` | One or more rejection reasons |
| `rejected_at` | Processing timestamp |

The rejected records must also be written to a separate HDFS location.

Recommended Iceberg table:

`retailmart.rejected_events`

One rejected occurrence must produce one rejection record, even when it has multiple reasons.

---

## 7. Deduplication rules

Deduplication must occur after validation.

### Rule D1 — Identical records

Identical valid lines with identical content must collapse into one record for curated processing.

### Rule D2 — Conflicting versions

For multiple valid records with the same `event_id` in the same input file:

1. Group records by `event_id`.
2. Compare `updated_at`.
3. Select the record with the latest `updated_at`.
4. Use only that winning record for the existing-table upsert.

The latest `updated_at` wins wherever the conflicting version occurs in the file.

### Rule D3 — Invalid versions

Invalid records cannot become winning versions.

An invalid incoming update must be rejected independently of its timestamp.

### Unresolved tie case

The official specification does not define what to do if two different valid versions of the same event have exactly the same latest `updated_at`.

No such tie was found in the supplied valid records.

Do not silently select an arbitrary version. Ask the instructor or agree on and document a deterministic tie-breaker before handling this case.

---

## 8. Curated Iceberg table

Selected table:

`retailmart.clickstream_events`

### Primary business key

`event_id`

### Partitioning

Partition by UTC event date:

```text
event_date = UTC_DATE(event_time)
```

The `event_date` is derived and should not be trusted from the incoming raw event.

### Upsert rules

| Incoming condition | Required operation |
|---|---|
| `event_id` absent from curated table | INSERT |
| Existing ID and incoming `event_time` strictly later | UPDATE all fields |
| Existing ID and incoming `event_time` equal | DROP |
| Existing ID and incoming `event_time` earlier | DROP |
| Incoming record invalid | REJECT |

### Critical distinction

`updated_at` and `event_time` have different responsibilities.

- `updated_at` chooses the winning version within the incoming file.
- `event_time` decides whether that winning valid record can replace the existing curated record.

Do not use `updated_at` to override the event-time upsert rule.

### Event-date changes

If an update changes `event_time`:

1. Update the existing event.
2. Recalculate `event_date`.
3. Remove its old representation from the current curated state.
4. Store it under its new event date.
5. Ensure it appears exactly once.

---

## 9. INSERT, UPDATE, DROP and REJECT metrics

These operations must be counted separately.

| Metric | Meaning |
|---|---|
| `insert_count` | New event IDs inserted |
| `update_count` | Existing event IDs replaced by later event times |
| `drop_count` | Valid records ignored because their event time is equal to or earlier than stored time |
| `reject_count` | Invalid input occurrences |
| `exact_duplicate_count` | Excess identical valid occurrences removed during deduplication |
| `conflict_resolution_count` | Conflicting valid versions resolved by `updated_at` |

A DROP is not a REJECT.

A duplicate removed during pre-upsert deduplication must not automatically be counted as an upsert DROP.

---

## 10. File-level idempotency and safe reruns

This is a critical requirement.

The official specification says that reprocessing either file, including Day 1 after Day 2, must produce:

- Zero new inserts
- Zero updates
- Zero drops

Simply rerunning the row-level upsert logic is insufficient because previously processed Day 1 records would otherwise be counted as drops after Day 2.

### Required solution: processing ledger

Assign each input file a stable batch identifier based on its content, preferably using SHA-256.

The processing system must track:

- Source filename
- Content hash
- Batch ID
- Raw landing status
- Spark processing status
- Iceberg commit status
- Druid synchronization status
- Overall completion status

### Expected behavior

If a fully completed source file is submitted again:

1. Identify its content hash.
2. Check the processing ledger.
3. Recognize the completed batch.
4. Skip already completed processing stages.
5. Do not run the upsert a second time.
6. Do not create duplicate Druid data.
7. Report zero new inserts, updates and drops.

For failed or partially completed runs, resume from the last verified successful stage rather than blindly repeating every stage.

The team must test the failure window between an Iceberg commit and recording its successful status. Do not claim exactly-once behavior without testing recovery at that boundary.

---

## 11. Druid synchronization contract

Druid must mirror the current Iceberg curated table.

After each successful Spark upsert:

1. Export the complete current curated state to Parquet.
2. Make the export available to Druid.
3. Synchronize Druid with the current state.
4. Verify that obsolete event versions are no longer represented.
5. Verify that each event appears exactly once.
6. Verify analytical totals against the curated table.

### Required behavior

Druid must correctly handle:

- New events
- Updated events
- Late-arriving events
- Event-date changes
- Reprocessing Day 1 after Day 2
- Reprocessing Day 2

Appending new Parquet files without replacing old analytical representations is not sufficient.

### Druid implementation decision

The exact batch replacement/overwrite mechanism must be verified with the selected Druid version and its Parquet/HDFS extensions.

Do not finalize the Druid ingestion configuration until a test proves that updating an event does not leave the old version contributing to analytics.

The official specification mentions the possible need for:

- `druid-hdfs-storage`
- `druid-parquet-extensions`

If Druid cannot access HDFS, configure the required Hadoop XML files in:

```text
$DRUID_HOME/conf/_common/
```

---

## 12. Business analytics definitions

### Purchase

```text
event_type = 'purchase'
```

### Revenue

```text
revenue = SUM(price * quantity)
```

Only purchase events contribute to revenue.

### Event date

```text
event_date = UTC_DATE(event_time)
```

### Dashboard requirements

The dashboard must provide:

1. Date-range filter.
2. Device filter.
3. Revenue KPI.
4. Purchase-count KPI.
5. Revenue by category bar chart.

Druid SQL must power the dashboard queries.

---

## 13. Team interface contracts

| Owner | Module | Input | Output |
|---|---|---|---|
| Gurudath | Producer | Day 1/Day 2 JSONL | Original lines published to Kafka |
| Gurudath / Tejas | Consumer | Kafka messages | Unchanged raw lines in HDFS |
| Praneeth | Spark validation | Raw HDFS files | Valid records and rejected records |
| Praneeth | Deduplication | Valid records | One winning valid record per event ID per batch |
| Praneeth | Iceberg upsert | Winning records + existing table | Updated current curated state and metrics |
| Praneeth | Export | Current Iceberg state | Full-state Parquet export |
| Shreyas | Druid | Curated Parquet export | Current-state analytical datasource |
| Shreyas | Dashboard | Druid SQL | KPIs, filters and category chart |
| Tejas | Airflow | Stage execution interfaces | Ordered, retryable pipeline |
| All | QA | Expected outputs | Verified acceptance evidence |

### Proposed stable command interfaces

Exact script filenames can be agreed during implementation, but the following parameters must be supported.

**Producer**

```text
--input
--topic
--batch-id
```

**Kafka consumer**

```text
--topic
--target-hdfs-path
--batch-id
```

**Spark processing**

```text
--input-path
--batch-id
--processing-day
```

**Druid synchronization**

```text
--export-path
--batch-id
```

Each script must return a meaningful exit code:

- `0`: successful completion
- Nonzero: failure requiring Airflow to mark the task as failed

All commands and configuration must be documented in the project README.

---

## 14. Proposed shared names

These are team conventions.

| Component | Name |
|---|---|
| Kafka topic | `retailmart_clickstream` |
| HDFS root | `/clickpulse/` |
| Raw directory | `/clickpulse/raw/` |
| Rejected directory | `/clickpulse/rejected/` |
| Export directory | `/clickpulse/exports/` |
| Iceberg database | `retailmart` |
| Curated table | `clickstream_events` |
| Rejected table | `rejected_events` |
| Druid datasource | `retailmart_clickstream` |
| Airflow DAG | `clickpulse_pipeline` |

---

## 15. Verified dataset baseline

The following values were calculated from the two supplied files using the official validation and processing rules.

They are our initial test reference values, not instructor-provided expected outputs.

### Raw and validation counts

| Measurement | Day 1 | Day 2 | Combined |
|---|---:|---:|---:|
| Raw lines | 144 | 148 | 292 |
| Readable JSON objects | 142 | 145 | 287 |
| Unreadable lines | 2 | 3 | 5 |
| Invalid JSON objects | 14 | 12 | 26 |
| Valid records | 128 | 133 | 261 |
| Rejected occurrences | 16 | 15 | 31 |

### Rejection reasons

Counts below are rejection-reason occurrences. A single invalid record can have multiple reasons.

| Reason | Day 1 | Day 2 |
|---|---:|---:|
| Missing/empty event ID | 2 | 1 |
| Missing/empty user ID | 1 | 2 |
| Missing/empty session ID | 1 | 1 |
| Invalid event type | 2 | 2 |
| Invalid event time | 2 | 2 |
| Negative price | 2 | 2 |
| Negative quantity | 4 | 2 |
| Unreadable JSON/non-object | 2 | 3 |

### Deduplication baseline

| Measurement | Day 1 | Day 2 |
|---|---:|---:|
| Valid input records | 128 | 133 |
| Exact duplicate excess | 6 | 4 |
| Unique event IDs after deduplication | 116 | 125 |
| Repeated event-ID groups | 11 | 8 |
| Conflicting event IDs | 6 | 4 |
| Conflicting latest `updated_at` ties | 0 | 0 |

### Day 1 and Day 2 upsert baseline

| Operation | Expected count |
|---|---:|
| Day 1 curated records | 116 |
| Day 2 valid unique event IDs | 125 |
| IDs shared between Day 1 and Day 2 | 24 |
| Day 2 INSERT | 101 |
| Day 2 UPDATE | 12 |
| Day 2 DROP | 12 |
| Day 2 REJECT | 15 |
| Final curated records | 217 |

The 12 drops consist of:

- 6 records with equal event time.
- 6 records with earlier event time.

The 12 updates change the stored event time. In the supplied data, these updates also exercise event-date movement.

Two invalid Day 2 updates target existing Day 1 records:

- `evt_129bdeb398` — negative price.
- `evt_b28a29110d` — negative quantity.

Both must be rejected, preserving the original valid records.

### Final current-state analytics baseline

| Measurement | Expected value |
|---|---:|
| Unique curated events | 217 |
| Purchase events | 33 |
| Total revenue | 10,061.31 |

Revenue by category:

| Category | Revenue |
|---|---:|
| Electronics | 7,158.29 |
| Sports | 835.15 |
| Home & Kitchen | 790.90 |
| Grocery | 571.93 |
| Fashion | 442.62 |
| Books | 210.22 |
| Beauty | 52.20 |

Revenue by device:

| Device | Purchases | Revenue |
|---|---:|---:|
| Mobile | 20 | 5,908.61 |
| Desktop | 10 | 3,437.05 |
| Tablet | 3 | 715.65 |

Final events by UTC date:

| Event date | Events |
|---|---:|
| 2025-01-01 | 57 |
| 2025-01-02 | 32 |
| 2025-01-03 | 26 |
| 2025-01-04 | 25 |
| 2025-01-05 | 77 |

Revenue values are in the dataset's price units; the official specification does not explicitly identify a currency.

---

## 16. Mandatory acceptance tests

No module is complete until its tests pass.

### Raw ingestion — Gurudath

- [ ] Day 1 produces 144 raw input occurrences.
- [ ] Day 2 produces 148 raw input occurrences.
- [ ] Original lines remain unchanged.
- [ ] Malformed lines are preserved.
- [ ] Identical duplicate occurrences are preserved in raw storage.
- [ ] Consumer restart does not lose input records.
- [ ] Repeated ingestion does not create unintended duplicate source batches.

### Validation and deduplication — Praneeth

- [ ] Day 1: 128 valid, 16 rejected.
- [ ] Day 2: 133 valid, 15 rejected.
- [ ] All specified validation rules are implemented.
- [ ] Multiple rejection reasons are retained.
- [ ] Day 1 deduplication produces 116 unique event IDs.
- [ ] Day 2 deduplication produces 125 unique event IDs.
- [ ] Latest `updated_at` wins among valid conflicting versions.
- [ ] Invalid versions cannot overwrite valid versions.

### Iceberg upserts — Praneeth

- [ ] Day 1 creates 116 curated records.
- [ ] Day 2 inserts 101 records.
- [ ] Day 2 updates 12 records.
- [ ] Day 2 drops 12 valid records.
- [ ] Day 2 rejects 15 occurrences.
- [ ] Final curated table contains 217 records.
- [ ] Both invalid-update examples preserve their original valid records.
- [ ] Event-date changes do not leave old event representations.
- [ ] Late-arriving new events are inserted even for earlier dates.

### Druid and dashboard — Shreyas

- [ ] Druid reflects the current curated state.
- [ ] Updated events appear exactly once.
- [ ] Old event versions do not contribute to revenue.
- [ ] Final event count is 217.
- [ ] Purchase count is 33.
- [ ] Revenue is 10,061.31.
- [ ] Revenue by category matches the reference.
- [ ] Device and date filters work.
- [ ] Druid reruns do not accumulate duplicate records.

### Airflow and reruns — Tejas

- [ ] Day 1 completes before Day 2.
- [ ] Task dependencies are correct.
- [ ] Failed tasks return nonzero exit codes.
- [ ] Retries are safe.
- [ ] Completed source batches are recognized by the processing ledger.
- [ ] Reprocessing Day 1 after Day 2 produces zero inserts, updates and drops.
- [ ] Reprocessing Day 2 produces zero inserts, updates and drops.
- [ ] A partial failure resumes from the last verified successful stage.

### Submission preservation — All members

- [ ] Final Day 1 run is completed before Day 2.
- [ ] Day 2 updates the existing curated table.
- [ ] No delete-and-reload strategy is used for the curated table.
- [ ] Kafka, HDFS, Iceberg and Druid are not reset before submission.
- [ ] Logs, screenshots and test outputs are preserved.

---

## 17. Open decisions — do not silently assume

These items need team confirmation or instructor clarification.

| Question | Current status |
|---|---|
| Equal latest `updated_at` with different content | Unspecified by assignment; absent in supplied valid data |
| Timezone-naive but otherwise parseable timestamps | Not present in supplied valid data; interpretation must be agreed |
| Exact Druid full-state replacement method | Must be tested with selected Druid version |
| Exact Spark/Hadoop/Iceberg version matrix | Pending compatibility test |
| Remote execution method for Airflow | Pending network and VM setup |
| Kafka partition count and replication configuration | Pending infrastructure design |
| HDFS replication setting | Must match actual DataNode count |
| Currency unit | Not specified by assignment |

No unresolved item should be hidden in code as an undocumented assumption.

---

## 18. Definition of contract approval

This document becomes the team's shared implementation contract after all four members review and approve it.

**Approval checklist:**

- [ ] Praneeth approves Spark/Iceberg rules.
- [ ] Gurudath approves Kafka/HDFS interfaces.
- [ ] Shreyas approves Druid/dashboard interfaces.
- [ ] Tejas approves Airflow/QA interfaces.

Any change to schemas, paths, processing rules or interfaces must be documented and communicated to all four members.

**Project principle: Build independently, integrate against shared contracts, and verify every claim with test evidence.**
