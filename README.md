# ClickPulse — Build Your Own Analytics System

ClickPulse is a distributed clickstream analytics pipeline. It ingests event records through Kafka, preserves original input in HDFS, validates and upserts current event state using Spark and Apache Iceberg, exports the current curated state for Apache Druid, and presents analytics in a Streamlit dashboard. Apache Airflow orchestrates  processing.

## Architecture

- **VM1:** Event producer; Kafka brokers 1 and 2 using KRaft.
- **VM2:** Kafka consumer/ingestion, HDFS NameNode  and DataNode, Airflow.
- **VM3:** Spark standalone master/worker and Spark jobs.
- **VM4:** Apache Druid and Streamlit dashboard.

Spark reads the persisted raw data from HDFS; it does not consume Kafka directly. Kafka brokers are long-running services and are not started or stopped by the Airflow DAG.

## Repository layout

```text
ClickPulse/
├── airflow/                 # Airflow DAGs and plugins
├── config/                  # Shared configuration templates
├── data/                    # Local sample/expected data only
├── docs/                    # Architecture, data contract, team assignments
├── evidence/                # Screenshots, logs, test evidence
├── logs/                    # Local development logs (not committed)
├── scripts/                 # Setup and developer utility scripts
├── src/clickpulse/
│   ├── common/              # Schema, validation, constants
│   ├── producer/            # Kafka event producer
│   ├── ingestion/           # Kafka consumer and raw HDFS writer
│   ├── spark/               # Validation, deduplication, upsert jobs
│   ├── storage/              # Iceberg table operations and batch ledger
│   ├── druid/               # Druid export and ingestion helpers
│   └── dashboard/           # Streamlit dashboard
├── tests/
│   ├── unit/
│   ├── integration/
│   └── fixtures/
├── .env.example
├── .gitignore
├── pyproject.toml
└── requirements.txt
```

## Initial setup

1. Create and activate a virtual environment.
2. Install Python dependencies.
3. Copy `.env.example` to `.env` and fill in environment-specific values.
4. Keep credentials and machine-specific addresses out of Git.
5. Read `docs/DATA_CONTRACT.md` before implementing any component.

### Windows PowerShell

```powershell
py -3.11 -m venv .venv
.\.venv\Scripts\Activate.ps1
python -m pip install --upgrade pip
pip install -r requirements.txt
Copy-Item .env.example .env
```

If PowerShell blocks activation, run this for the current terminal only:

```powershell
Set-ExecutionPolicy -Scope Process -ExecutionPolicy Bypass
```

## Run tests

```powershell
python -m pytest
```

## Important data rules

- Preserve every original input line in raw storage, including malformed lines and duplicates.
- Store rejected occurrences separately with their rejection reasons.
- Deduplicate within a batch by `event_id`, selecting the greatest `updated_at`.
- Across batches, `event_time` determines whether a valid incoming record updates current state.
- Invalid updates must not overwrite existing valid records.
- Use a batch-processing ledger so replaying a completed input file is a no-op.
- Druid must represent the current curated state, not an append-only history of stale versions.

The authoritative field definitions, validation rules, upsert semantics, and expected dataset results belong in `docs/DATA_CONTRACT.md`.
