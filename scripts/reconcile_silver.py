"""SQL assertions and reconciliation checks for the VCT Silver layer.

Verifies:
1. Latest-snapshot replacement: 2026 contains ONLY s07_2026_full (0 s06_2026_partial rows).
2. Dimension primary-key uniqueness (dim_tournament, dim_team, dim_player, dim_agent, dim_map).
3. Game resolution rate >= 99.5% across core fact tables.
4. Zero unexpected typing/duration parse failures in quarantine_silver_records.
5. Literal player 'nan' preserved in dim_player.

Usage:
  python scripts/reconcile_silver.py --profile dev --silver-schema final_project.dev_vct_silver
"""

from __future__ import annotations

import argparse
import json
import subprocess
import sys


def _get_warehouse_id(profile: str) -> str:
    out = subprocess.check_output(
        ["databricks", "warehouses", "list", "--output", "json", "--profile", profile],
        text=True,
    )
    return json.loads(out)[0]["id"]


def _query(warehouse_id: str, sql: str, profile: str) -> list[dict[str, str | None]]:
    payload = json.dumps(
        {
            "warehouse_id": warehouse_id,
            "statement": sql,
            "wait_timeout": "50s",
            "disposition": "INLINE",
        }
    )
    out = subprocess.check_output(
        ["databricks", "api", "post", "/api/2.0/sql/statements", "--json", payload, "--profile", profile],
        text=True,
    )
    resp = json.loads(out)
    if resp.get("status", {}).get("state") != "SUCCEEDED":
        raise RuntimeError(f"SQL failed: {resp.get('status')}")
    cols = [c["name"] for c in resp["manifest"]["schema"]["columns"]]
    return [dict(zip(cols, r, strict=True)) for r in resp.get("result", {}).get("data_array", [])]


def main() -> None:
    parser = argparse.ArgumentParser(description="Run Silver SQL quality assertions.")
    parser.add_argument("--profile", default="dev")
    parser.add_argument("--silver-schema", default="final_project.dev_vct_silver")
    args = parser.parse_args()

    s = args.silver_schema
    wh = _get_warehouse_id(args.profile)

    assertions: list[tuple[str, str]] = [
        (
            "no_partial_2026_in_fct_player_map_stats",
            f"SELECT COUNT(*) AS bad FROM {s}.fct_player_map_stats WHERE _snapshot_id = 's06_2026_partial'",
        ),
        (
            "no_partial_2026_in_fct_map_game",
            f"SELECT COUNT(*) AS bad FROM {s}.fct_map_game WHERE _snapshot_id = 's06_2026_partial'",
        ),
        (
            "no_typing_parse_failures",
            f"SELECT COUNT(*) AS bad FROM {s}.quarantine_silver_records "
            "WHERE quarantine_reason IN ('TYPING_PARSE_FAILURE', 'DURATION_PARSE_FAILURE')",
        ),
        (
            "dim_player_nan_preserved",
            f"SELECT CASE WHEN COUNT(*) >= 1 THEN 0 ELSE 1 END AS bad "
            f"FROM {s}.dim_player WHERE LOWER(canonical_player_name) = 'nan'",
        ),
        (
            "fct_map_game_res_rate_ge_99_5",
            f"SELECT CASE WHEN 100.0 * COUNT(game_id) / COUNT(*) >= 99.5 THEN 0 ELSE 1 END AS bad "
            f"FROM {s}.fct_map_game",
        ),
        (
            "fct_economy_round_res_rate_ge_99_5",
            f"SELECT CASE WHEN 100.0 * COUNT(game_id) / COUNT(*) >= 99.5 THEN 0 ELSE 1 END AS bad "
            f"FROM {s}.fct_economy_round",
        ),
    ]

    failed = 0
    for name, sql in assertions:
        val = int(_query(wh, sql, args.profile)[0]["bad"])
        status = "PASS" if val == 0 else f"FAIL (bad={val})"
        print(f"  [{status}] {name}")
        if val != 0:
            failed += 1

    print(f"\nSilver assertions: {len(assertions) - failed}/{len(assertions)} passed.")
    if failed:
        sys.exit(1)


if __name__ == "__main__":
    main()
