"""Silver aggregate/rollup facts, unified quarantine table, and pipeline data-quality checks."""

from __future__ import annotations

from pyspark import pipelines as dp
from pyspark.sql import functions as F

from pipelines.silver.silver_transforms import (
    clean_str,
    documented_gap_reason_expr,
    latest_snapshot_by_year,
    parse_double,
    parse_int,
    parse_pct,
    parse_ratio_played,
    parse_ratio_won,
    surrogate_key,
)

BRONZE = spark.conf.get("vct.bronze_schema")
SILVER = spark.conf.get("vct.silver_schema")


def _join_stage_match_type(df):
    """Join non-rollup rows to dim_stage_match_type (all_ids superset); rollup rows keep NULL IDs."""
    dim = spark.read.table(f"{SILVER}.dim_stage_match_type").select(
        F.col("year").alias("_dy"),
        F.col("tournament_name").alias("_dt"),
        F.col("stage_name").alias("_ds"),
        F.col("match_type_name").alias("_dmt"),
        F.col("tournament_id").alias("tournament_id"),
        F.col("stage_id").alias("stage_id"),
        F.col("match_type_id").alias("match_type_id"),
        F.col("is_showmatch").alias("is_showmatch"),
    )
    return df.join(
        dim,
        (df["year"] == dim["_dy"])
        & (df["tournament"] == dim["_dt"])
        & (df["stage"] == dim["_ds"])
        & (df["match_type"] == dim["_dmt"])
        & (~df["is_tournament_rollup"]),
        how="left",
    ).drop("_dy", "_dt", "_ds", "_dmt")


@dp.materialized_view(
    name=f"{SILVER}.fct_agent_pick_rate",
    comment="Agent pick rates by tournament/stage/match_type/map with explicit rollup flags.",
    table_properties={"quality": "silver"},
)
@dp.expect("valid_pick_rate", "pick_rate IS NULL OR (pick_rate >= 0.0 AND pick_rate <= 1.0)")
def fct_agent_pick_rate():
    src = (
        latest_snapshot_by_year(spark.read.table(f"{BRONZE}.agents_pick_rates"))
        .select(
            F.col("_year").alias("year"),
            F.col("_snapshot_id").alias("_snapshot_id"),
            clean_str("tournament").alias("tournament"),
            clean_str("stage").alias("stage"),
            clean_str("match_type").alias("match_type"),
            clean_str("map").alias("map_name"),
            F.lower(clean_str("agent")).alias("agent"),
            parse_pct("pick_rate").alias("pick_rate"),
        )
        .withColumn(
            "is_tournament_rollup",
            (F.col("stage") == "All Stages") | (F.col("match_type") == "All Match Types"),
        )
        .withColumn("is_map_rollup", F.col("map_name") == "All Maps")
    )
    return _join_stage_match_type(src)


@dp.materialized_view(
    name=f"{SILVER}.fct_agent_map_side_stats",
    comment="Map attacker vs defender win percentages by tournament/stage/match_type with is_tournament_rollup flag.",
    table_properties={"quality": "silver"},
)
def fct_agent_map_side_stats():
    src = (
        latest_snapshot_by_year(spark.read.table(f"{BRONZE}.agents_maps_stats"))
        .select(
            F.col("_year").alias("year"),
            F.col("_snapshot_id").alias("_snapshot_id"),
            clean_str("tournament").alias("tournament"),
            clean_str("stage").alias("stage"),
            clean_str("match_type").alias("match_type"),
            clean_str("map").alias("map_name"),
            parse_int("total_maps_played").alias("total_maps_played"),
            parse_pct("attacker_side_win_percentage").alias("attacker_win_pct"),
            parse_pct("defender_side_win_percentage").alias("defender_win_pct"),
        )
        .withColumn(
            "is_tournament_rollup",
            (F.col("stage") == "All Stages") | (F.col("match_type") == "All Match Types"),
        )
    )
    return _join_stage_match_type(src)


@dp.materialized_view(
    name=f"{SILVER}.fct_agent_team_pick",
    comment="Team agent pick counts and map win/loss totals with rollup flags.",
    table_properties={"quality": "silver"},
)
def fct_agent_team_pick():
    src = (
        latest_snapshot_by_year(spark.read.table(f"{BRONZE}.agents_teams_picked"))
        .select(
            F.col("_year").alias("year"),
            F.col("_snapshot_id").alias("_snapshot_id"),
            clean_str("tournament").alias("tournament"),
            clean_str("stage").alias("stage"),
            clean_str("match_type").alias("match_type"),
            clean_str("map").alias("map_name"),
            clean_str("team").alias("team_name"),
            F.lower(clean_str("agent")).alias("agent"),
            parse_int("total_wins_by_map").alias("total_wins_by_map"),
            parse_int("total_loss_by_map").alias("total_loss_by_map"),
            parse_int("total_maps_played").alias("total_maps_played"),
        )
        .withColumn(
            "is_tournament_rollup",
            (F.col("stage") == "All Stages") | (F.col("match_type") == "All Match Types"),
        )
        .withColumn("is_map_rollup", F.col("map_name") == "All Maps")
    )
    df = _join_stage_match_type(src)
    yearly_team = spark.read.table(f"{SILVER}.ref_yearly_team_lookup").select(
        F.col("year").alias("_ty"),
        F.col("team_name").alias("_tn"),
        F.col("team_id").alias("_tid"),
    )
    return (
        df.join(yearly_team, (df["year"] == yearly_team["_ty"]) & (df["team_name"] == yearly_team["_tn"]), how="left")
        .withColumn("team_id", F.coalesce(F.col("_tid"), surrogate_key("SK_TEAM", F.col("team_name"))))
        .drop("_ty", "_tn", "_tid")
    )


@dp.materialized_view(
    name=f"{SILVER}.fct_player_tournament_stats",
    comment="Player tournament/stage summary stats with typed ratios/percents and is_tournament_rollup.",
    table_properties={"quality": "silver"},
)
def fct_player_tournament_stats():
    src = (
        latest_snapshot_by_year(spark.read.table(f"{BRONZE}.players_stats"))
        .select(
            F.col("_year").alias("year"),
            F.col("_snapshot_id").alias("_snapshot_id"),
            clean_str("tournament").alias("tournament"),
            clean_str("stage").alias("stage"),
            clean_str("match_type").alias("match_type"),
            clean_str("player").alias("player_name"),
            clean_str("teams").alias("teams_raw"),
            clean_str("agents").alias("agents_raw"),
            parse_int("rounds_played").alias("rounds_played"),
            parse_double("rating").alias("rating"),
            parse_double("average_combat_score").alias("acs"),
            parse_double("kills_deaths").alias("kd_ratio"),
            parse_pct("kill_assist_trade_survive_pct").alias("kast_pct"),
            parse_double("average_damage_per_round").alias("adr"),
            parse_double("kills_per_round").alias("kills_per_round"),
            parse_double("assists_per_round").alias("assists_per_round"),
            parse_double("first_kills_per_round").alias("first_kills_per_round"),
            parse_double("first_deaths_per_round").alias("first_deaths_per_round"),
            parse_pct("headshot_pct").alias("hs_pct"),
            parse_pct("clutch_success_pct").alias("clutch_success_pct"),
            parse_ratio_won("clutches_won_played").alias("clutches_won"),
            parse_ratio_played("clutches_won_played").alias("clutches_played"),
            parse_int("maximum_kills_in_a_single_map").alias("max_kills_single_map"),
            parse_int("kills").alias("kills"),
            parse_int("deaths").alias("deaths"),
            parse_int("assists").alias("assists"),
            parse_int("first_kills").alias("first_kills"),
            parse_int("first_deaths").alias("first_deaths"),
        )
        .withColumn(
            "is_tournament_rollup",
            (F.col("stage") == "All Stages") | (F.col("match_type") == "All Match Types"),
        )
    )
    df = _join_stage_match_type(src)
    yearly_player = spark.read.table(f"{SILVER}.ref_yearly_player_lookup").select(
        F.col("year").alias("_py"),
        F.col("player_name").alias("_pn"),
        F.col("player_id").alias("_pid"),
        F.col("has_source_id").alias("player_has_source_id"),
    )
    return (
        df.join(
            yearly_player,
            (df["year"] == yearly_player["_py"]) & (df["player_name"] == yearly_player["_pn"]),
            how="left",
        )
        .withColumn(
            "player_id",
            F.coalesce(F.col("_pid"), surrogate_key("SK_PLAYER", F.col("year"), F.col("player_name"))),
        )
        .withColumn("player_has_source_id", F.coalesce(F.col("player_has_source_id"), F.lit(False)))
        .withColumn("documented_gap_reason", documented_gap_reason_expr("tournament"))
        .drop("_py", "_pn", "_pid")
    )


@dp.materialized_view(
    name=f"{SILVER}.quarantine_silver_records",
    comment="Unified Silver quarantine: ambiguous game-key fact rows + any typing parse failures with reason.",
    table_properties={"quality": "silver", "layer": "quarantine"},
)
def quarantine_silver_records():
    ambig_keys = spark.read.table(f"{SILVER}.quarantine_ambiguous_games").select(
        "year", "tournament", "stage", "match_type", "match_name", "map"
    )

    # 1. Ambiguous game-key rows from matches_maps_scores
    maps_ambig = (
        latest_snapshot_by_year(spark.read.table(f"{BRONZE}.matches_maps_scores"))
        .withColumn("year", F.col("_year"))
        .join(ambig_keys, on=["year", "tournament", "stage", "match_type", "match_name", "map"], how="inner")
        .select(
            F.lit("matches_maps_scores").alias("source_table"),
            F.lit("AMBIGUOUS_GAME_KEY").alias("quarantine_reason"),
            F.col("_year").alias("year"),
            F.col("_snapshot_id").alias("_snapshot_id"),
            F.to_json(F.struct("tournament", "stage", "match_type", "match_name", "map", "team_a", "team_b")).alias(
                "raw_record_json"
            ),
        )
    )

    # 2. Ambiguous game-key rows from matches_overview
    overview_ambig = (
        latest_snapshot_by_year(spark.read.table(f"{BRONZE}.matches_overview"))
        .filter(F.col("map") != "All Maps")
        .withColumn("year", F.col("_year"))
        .join(ambig_keys, on=["year", "tournament", "stage", "match_type", "match_name", "map"], how="inner")
        .select(
            F.lit("matches_overview").alias("source_table"),
            F.lit("AMBIGUOUS_GAME_KEY").alias("quarantine_reason"),
            F.col("_year").alias("year"),
            F.col("_snapshot_id").alias("_snapshot_id"),
            F.to_json(F.struct("tournament", "stage", "match_type", "match_name", "map", "player", "team")).alias(
                "raw_record_json"
            ),
        )
    )

    # 3. Parse-failure audit on key typed columns in matches_overview (non-empty raw string that failed try_cast)
    overview_parse_fail = (
        latest_snapshot_by_year(spark.read.table(f"{BRONZE}.matches_overview"))
        .filter(
            (clean_str("kills").isNotNull() & parse_int("kills").isNull())
            | (clean_str("deaths").isNotNull() & parse_int("deaths").isNull())
            | (clean_str("headshot_pct").isNotNull() & parse_pct("headshot_pct").isNull())
            | (clean_str("rating").isNotNull() & parse_double("rating").isNull())
        )
        .select(
            F.lit("matches_overview").alias("source_table"),
            F.lit("TYPING_PARSE_FAILURE").alias("quarantine_reason"),
            F.col("_year").alias("year"),
            F.col("_snapshot_id").alias("_snapshot_id"),
            F.to_json(
                F.struct(
                    "tournament",
                    "match_name",
                    "map",
                    "player",
                    "kills",
                    "deaths",
                    "headshot_pct",
                    "rating",
                )
            ).alias("raw_record_json"),
        )
    )

    # 4. Parse-failure audit on matches_maps_scores duration
    dur_regex = r"^\d+:\d{2}(:\d{2})?$"
    duration_parse_fail = (
        latest_snapshot_by_year(spark.read.table(f"{BRONZE}.matches_maps_scores"))
        .filter(
            clean_str("duration").isNotNull()
            & F.regexp_extract(clean_str("duration"), dur_regex, 0).eqNullSafe("")
        )
        .select(
            F.lit("matches_maps_scores").alias("source_table"),
            F.lit("DURATION_PARSE_FAILURE").alias("quarantine_reason"),
            F.col("_year").alias("year"),
            F.col("_snapshot_id").alias("_snapshot_id"),
            F.to_json(F.struct("tournament", "match_name", "map", "duration")).alias("raw_record_json"),
        )
    )

    return maps_ambig.unionByName(overview_ambig).unionByName(overview_parse_fail).unionByName(duration_parse_fail)


@dp.materialized_view(
    name="dq_silver_dimension_uniqueness",
    private=True,
    comment="Pipeline aggregate expectation verifying primary-key uniqueness on Silver conformed dimensions.",
)
@dp.expect_or_fail("dim_keys_unique", "duplicate_key_count = 0")
def dq_silver_dimension_uniqueness():
    checks = [
        ("dim_tournament", "tournament_id"),
        ("dim_team", "team_id"),
        ("dim_player", "player_id"),
        ("dim_agent", "agent"),
        ("dim_map", "map_name"),
    ]
    dfs = []
    for table, key in checks:
        df = spark.read.table(f"{SILVER}.{table}")
        dfs.append(
            df.agg(
                F.lit(table).alias("table_name"),
                (F.count("*") - F.countDistinct(key)).alias("duplicate_key_count"),
            )
        )
    out = dfs[0]
    for nxt in dfs[1:]:
        out = out.unionByName(nxt)
    return out


@dp.materialized_view(
    name="dq_silver_fk_integrity",
    private=True,
    comment="Pipeline FK check: ensures resolved tournament_ids in fct_map_game exist in dim_tournament.",
)
@dp.expect_or_fail("fct_map_game_tournament_fk", "orphan_count = 0")
def dq_silver_fk_integrity():
    orphans = (
        spark.read.table(f"{SILVER}.fct_map_game")
        .filter(F.col("tournament_id").isNotNull())
        .select("tournament_id")
        .distinct()
        .join(spark.read.table(f"{SILVER}.dim_tournament").select("tournament_id"), on="tournament_id", how="left_anti")
    )
    return orphans.agg(F.count("*").alias("orphan_count"))
