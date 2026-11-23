"""Serverless job task: end-to-end SQL data-quality assertions across Bronze, Silver, and Gold."""

from __future__ import annotations

import argparse
import sys

from pyspark.sql import SparkSession

BRONZE_TABLES: tuple[str, ...] = (
    "agents_pick_rates",
    "agents_maps_stats",
    "agents_teams_picked",
    "ids_players",
    "ids_teams",
    "ids_tournament_stage_match_types",
    "ids_tournament_stage_matches_games",
    "matches_draft_phase",
    "matches_eco_rounds",
    "matches_eco_stats",
    "matches_kills",
    "matches_kills_stats",
    "matches_maps_played",
    "matches_maps_scores",
    "matches_overview",
    "matches_rounds_kills",
    "matches_scores",
    "matches_team_mapping",
    "matches_win_loss_methods_count",
    "matches_win_loss_methods_round_number",
    "players_stats",
    "all_ids_matches_games",
    "all_ids_players",
    "all_ids_teams",
    "all_ids_teams_mapping",
    "all_ids_tournament_stage_match_types",
)

GOLD_TABLES: tuple[str, ...] = (
    "dim_tournament",
    "dim_team",
    "dim_player",
    "dim_agent",
    "dim_map",
    "fct_game_scores",
    "fct_player_game_stats",
    "match_results_base",
    "player_career_summary",
    "player_season_form",
    "team_results_timeline",
    "team_map_pool_winrate",
    "agent_meta_by_tournament",
    "map_side_balance",
    "economy_efficiency_by_team",
)


def main() -> None:
    parser = argparse.ArgumentParser(description="Run Bronze/Silver/Gold SQL quality assertions.")
    parser.add_argument("--bronze-schema", required=True)
    parser.add_argument("--silver-schema", required=True)
    parser.add_argument("--gold-schema", required=True)
    args = parser.parse_args()

    spark = SparkSession.builder.getOrCreate()
    b, s, g = args.bronze_schema, args.silver_schema, args.gold_schema

    assertions: list[tuple[str, str]] = []

    # 1. Bronze: zero rescued rows and all 26 tables populated
    rescued_union = " UNION ALL ".join(
        f"SELECT COUNT(_rescued_data) AS c FROM {b}.{t}" for t in BRONZE_TABLES
    )
    assertions.append(("bronze_zero_rescued_data", f"SELECT SUM(c) AS bad FROM ({rescued_union})"))

    bronze_empty_union = " UNION ALL ".join(
        f"SELECT CASE WHEN COUNT(*) > 0 THEN 0 ELSE 1 END AS c FROM {b}.{t}" for t in BRONZE_TABLES
    )
    assertions.append(("bronze_all_26_tables_non_empty", f"SELECT SUM(c) AS bad FROM ({bronze_empty_union})"))

    # 2. Silver: latest-snapshot replacement, uniqueness, parse failures, resolution rates, 'nan' player
    assertions.extend(
        [
            (
                "silver_no_partial_2026_in_player_map_stats",
                f"SELECT COUNT(*) AS bad FROM {s}.fct_player_map_stats WHERE _snapshot_id = 's06_2026_partial'",
            ),
            (
                "silver_no_partial_2026_in_map_game",
                f"SELECT COUNT(*) AS bad FROM {s}.fct_map_game WHERE _snapshot_id = 's06_2026_partial'",
            ),
            (
                "silver_no_typing_parse_failures",
                f"SELECT COUNT(*) AS bad FROM {s}.quarantine_silver_records "
                "WHERE quarantine_reason IN ('TYPING_PARSE_FAILURE', 'DURATION_PARSE_FAILURE')",
            ),
            (
                "silver_dim_player_nan_preserved",
                f"SELECT CASE WHEN COUNT(*) >= 1 THEN 0 ELSE 1 END AS bad "
                f"FROM {s}.dim_player WHERE LOWER(canonical_player_name) = 'nan'",
            ),
            (
                "silver_fct_map_game_res_ge_99_5",
                f"SELECT CASE WHEN 100.0 * COUNT(game_id) / COUNT(*) >= 99.5 THEN 0 ELSE 1 END AS bad "
                f"FROM {s}.fct_map_game",
            ),
            (
                "silver_fct_economy_round_res_ge_99_5",
                f"SELECT CASE WHEN 100.0 * COUNT(game_id) / COUNT(*) >= 99.5 THEN 0 ELSE 1 END AS bad "
                f"FROM {s}.fct_economy_round",
            ),
        ]
    )

    # 3. Gold: all 15 tables populated, no pre-release agent rows, valid draw/coverage invariants
    gold_empty_union = " UNION ALL ".join(
        f"SELECT CASE WHEN COUNT(*) > 0 THEN 0 ELSE 1 END AS c FROM {g}.{t}" for t in GOLD_TABLES
    )
    assertions.append(("gold_all_15_tables_non_empty", f"SELECT SUM(c) AS bad FROM ({gold_empty_union})"))
    assertions.extend(
        [
            (
                "gold_agent_meta_no_pre_release_rows",
                f"SELECT COUNT(*) AS bad FROM {g}.agent_meta_by_tournament WHERE year < agent_first_seen_year",
            ),
            (
                "gold_match_draws_have_null_winner",
                f"SELECT COUNT(*) AS bad FROM {g}.match_results_base "
                "WHERE is_draw = true AND winner_team_id IS NOT NULL",
            ),
            (
                "gold_player_career_valid_coverage_ratio",
                f"SELECT COUNT(*) AS bad FROM {g}.player_career_summary "
                "WHERE rating_coverage_pct < 0.0 OR rating_coverage_pct > 1.0",
            ),
        ]
    )

    failed = 0
    for name, sql in assertions:
        bad = int(spark.sql(sql).collect()[0]["bad"])
        status = "PASS" if bad == 0 else f"FAIL (bad={bad})"
        print(f"[{status}] {name}")
        if bad != 0:
            failed += 1

    print(f"\nData quality checks: {len(assertions) - failed}/{len(assertions)} passed.")
    if failed:
        sys.exit(1)


if __name__ == "__main__":
    main()
