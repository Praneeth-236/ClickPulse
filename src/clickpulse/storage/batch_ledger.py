"""File-level idempotency ledger.

A completed source batch must be recognized by a stable content hash and
replayed as a true no-op, rather than counted as row-level drops.
"""

from __future__ import annotations

import hashlib
import json
import os
import subprocess
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

LOCAL_LEDGER_DIR = Path(
    os.getenv("CLICKPULSE_LEDGER_DIR", Path.home() / "ClickPulse" / ".ledger")
)
HDFS_LEDGER_DIR = "/clickpulse/metadata/processing-ledger"


def get_batch_id(file_bytes: bytes) -> str:
    """Return a deterministic SHA-256 identifier for the exact source file."""
    return hashlib.sha256(file_bytes).hexdigest()


def _ledger_file(batch_id: str) -> Path:
    LOCAL_LEDGER_DIR.mkdir(parents=True, exist_ok=True)
    return LOCAL_LEDGER_DIR / f"{batch_id}.json"


def load_batch_record(batch_id: str) -> dict[str, Any] | None:
    """Load the durable ledger record from local disk or HDFS."""
    path = _ledger_file(batch_id)
    if path.exists():
        return json.loads(path.read_text(encoding="utf-8"))

    res = subprocess.run(
        ["hdfs", "dfs", "-cat", f"{HDFS_LEDGER_DIR}/{batch_id}.json"],
        capture_output=True,
        text=True,
    )
    if res.returncode == 0 and res.stdout.strip():
        record = json.loads(res.stdout)
        path.write_text(json.dumps(record, indent=2), encoding="utf-8")
        return record
    return None


def update_batch_stage(
    batch_id: str,
    stage: str,
    status: str,
    extra: dict[str, Any] | None = None,
) -> dict[str, Any]:
    """Persist stage progress locally and sync to HDFS metadata ledger."""
    record = load_batch_record(batch_id) or {
        "batch_id": batch_id,
        "created_at": datetime.now(timezone.utc).isoformat(),
        "stages": {},
    }
    record["updated_at"] = datetime.now(timezone.utc).isoformat()
    record["stages"][stage] = {
        "status": status,
        "timestamp": record["updated_at"],
    }
    if extra:
        record.update(extra)

    payload = json.dumps(record, indent=2)
    local_path = _ledger_file(batch_id)
    local_path.write_text(payload, encoding="utf-8")

    subprocess.run(["hdfs", "dfs", "-mkdir", "-p", HDFS_LEDGER_DIR], check=False)
    subprocess.run(
        ["hdfs", "dfs", "-put", "-f", str(local_path), f"{HDFS_LEDGER_DIR}/{batch_id}.json"],
        check=False,
    )
    return record


def is_batch_completed(batch_id: str) -> bool:
    """Check the durable processing ledger for full pipeline completion."""
    record = load_batch_record(batch_id)
    if not record:
        return False
    return record.get("overall_status") == "completed" or (
        record.get("stages", {}).get("druid", {}).get("status") == "completed"
    )
