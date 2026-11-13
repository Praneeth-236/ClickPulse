"""Export the complete configured Iceberg current state as Parquet."""

import json
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))

from dotenv import load_dotenv  # noqa: E402

from clickpulse.druid.export_current_state import export_current_state  # noqa: E402


def main() -> int:
    load_dotenv(ROOT / ".env")
    try:
        result = export_current_state()
    except Exception as exc:
        print(json.dumps({"status": "failed", "error": str(exc)}, sort_keys=True))
        return 1
    print(
        json.dumps(
            {
                "status": "succeeded",
                "source_table": result.source_table,
                "destination": result.destination,
                "record_count": result.record_count,
                "schema": json.loads(result.schema_json),
            },
            sort_keys=True,
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
