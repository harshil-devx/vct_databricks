"""Reconcile Bronze table row counts against local CSVs and check _rescued_data.

Uses the Databricks SQL Statement Execution API (`databricks api post /api/2.0/sql/statements`)
against the workspace SQL warehouse so every table/snapshot count is verified directly.

Usage:
  python scripts/reconcile_bronze.py --data-root "D:/DATABRICKS PROJECT" --profile dev
"""

from __future__ import annotations

import argparse
import csv
import json
import pathlib
import subprocess
import sys

sys.path.insert(0, str(pathlib.Path(__file__).resolve().parents[1] / "src"))

from pipelines.bronze.bronze_config import BRONZE_TABLES  # noqa: E402


def _get_warehouse_id(profile: str) -> str:
    out = subprocess.check_output(
        ["databricks", "warehouses", "list", "--output", "json", "--profile", profile],
        text=True,
    )
    warehouses = json.loads(out)
    if not warehouses:
        raise RuntimeError("No SQL warehouse found in workspace")
    return warehouses[0]["id"]


def _execute_sql(warehouse_id: str, statement: str, profile: str) -> list[dict[str, str | None]]:
    payload = json.dumps(
        {
            "warehouse_id": warehouse_id,
            "statement": statement,
            "wait_timeout": "50s",
            "disposition": "INLINE",
        }
    )
    out = subprocess.check_output(
        [
            "databricks",
            "api",
            "post",
            "/api/2.0/sql/statements",
            "--json",
            payload,
            "--profile",
            profile,
        ],
        text=True,
    )
    resp = json.loads(out)
    state = resp.get("status", {}).get("state")
    if state != "SUCCEEDED":
        raise RuntimeError(f"SQL failed ({state}): {resp.get('status', {}).get('error')}")
    cols = [c["name"] for c in resp["manifest"]["schema"]["columns"]]
    rows = resp.get("result", {}).get("data_array", [])
    return [dict(zip(cols, r, strict=True)) for r in rows]


def _local_csv_rows(path: pathlib.Path) -> int:
    with path.open("r", encoding="utf-8-sig", newline="") as f:
        reader = csv.reader(f)
        try:
            next(reader)
        except StopIteration:
            return 0
        return sum(1 for _ in reader)


def _expected_rows(data_root: pathlib.Path, rel_path: str, is_yearly: bool, snapshot_id: str) -> int:
    if not is_yearly:
        return _local_csv_rows(data_root / rel_path)
    year = snapshot_id.split("_")[1]
    total = _local_csv_rows(data_root / f"vct_{year}" / rel_path)
    if snapshot_id == "s06_2026_partial":
        return int(total * 0.8)
    return total


def main() -> None:
    parser = argparse.ArgumentParser(description="Reconcile Bronze tables against local CSVs.")
    parser.add_argument("--data-root", type=pathlib.Path, required=True)
    parser.add_argument("--profile", default="dev")
    parser.add_argument("--bronze-schema", default="final_project.dev_vct_bronze")
    args = parser.parse_args()

    warehouse_id = _get_warehouse_id(args.profile)

    # Single UNION ALL query across all 26 tables for speed.
    parts = [
        (
            f"SELECT '{spec.table_name}' AS table_name, _snapshot_id, _year, "
            f"COUNT(*) AS bronze_rows, COUNT(_rescued_data) AS rescued_rows "
            f"FROM {args.bronze_schema}.{spec.table_name} GROUP BY 1, 2, 3"
        )
        for spec in BRONZE_TABLES
    ]
    sql = "\nUNION ALL\n".join(parts) + "\nORDER BY table_name, _snapshot_id"
    rows = _execute_sql(warehouse_id, sql, args.profile)

    spec_by_name = {s.table_name: s for s in BRONZE_TABLES}
    mismatches = 0
    rescued_total = 0

    print(f"{'Table':<42} {'Snapshot':<18} {'Year':<6} {'Local':>10} {'Bronze':>10} {'Rescued':>8} {'Status'}")
    print("-" * 108)
    seen_tables: set[str] = set()
    for row in rows:
        tname = row["table_name"]
        snap = row["_snapshot_id"]
        year = row["_year"] or "-"
        bronze_cnt = int(row["bronze_rows"])
        rescued_cnt = int(row["rescued_rows"])
        spec = spec_by_name[tname]
        local_cnt = _expected_rows(args.data_root, spec.rel_path, spec.is_yearly, snap)
        ok = bronze_cnt == local_cnt and rescued_cnt == 0
        if not ok:
            mismatches += 1
        rescued_total += rescued_cnt
        seen_tables.add(tname)
        status = "OK" if ok else "MISMATCH"
        print(f"{tname:<42} {snap:<18} {year:<6} {local_cnt:>10,} {bronze_cnt:>10,} {rescued_cnt:>8,} {status}")

    empty_tables = [s.table_name for s in BRONZE_TABLES if s.table_name not in seen_tables]
    if empty_tables:
        print(f"\nTables with 0 rows (not yet uploaded): {', '.join(empty_tables)}")

    print(
        f"\nSummary: {len(rows)} table-snapshot groups checked, "
        f"{mismatches} mismatches, {rescued_total} rescued rows."
    )
    if mismatches > 0:
        sys.exit(1)


if __name__ == "__main__":
    main()
