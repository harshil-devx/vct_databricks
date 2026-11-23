"""Silver match-, map-, round-, and player-map-level fact tables (one grain each)."""

from __future__ import annotations

from pyspark import pipelines as dp
from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from pipelines.silver.silver_transforms import (
    canonical_win_loss_method_expr,
    clean_str,
    documented_gap_reason_expr,
    latest_snapshot_by_year,
    parse_credits,
    parse_double,
    parse_duration_sec,
    parse_int,
    parse_pct,
    surrogate_key,
)

BRONZE = spark.conf.get("vct.bronze_schema")
SILVER = spark.conf.get("vct.silver_schema")


def _normalize_tournament(df: DataFrame) -> DataFrame:
    """Apply seed_tournament_alias to normalize legacy tournament names before ID joins."""
    aliases = spark.read.table(f"{SILVER}.seed_tournament_alias").select(
        F.col("year").alias("_alias_year"),
        F.col("source_tournament").alias("_alias_src"),
        F.col("canonical_tournament").alias("_alias_canon"),
    )
    return (
        df.join(
            aliases,
            (df["year"] == aliases["_alias_year"]) & (df["tournament"] == aliases["_alias_src"]),
            how="left",
        )
        .withColumn("tournament", F.coalesce(F.col("_alias_canon"), F.col("tournament")))
        .drop("_alias_year", "_alias_src", "_alias_canon")
    )


def _resolve_team(df: DataFrame, name_col: str, id_out: str, has_src_out: str | None = None) -> DataFrame:
    """Resolve team name -> team_id via yearly ids, then team_alias, then deterministic surrogate.

    Placeholder 'TBD' is kept as team_id='TBD' (excluded from real team joins).
    """
    yearly = spark.read.table(f"{SILVER}.ref_yearly_team_lookup").select(
        F.col("year").alias("_ty"),
        F.col("team_name").alias("_tn"),
        F.col("team_id").alias("_tid"),
        F.col("has_source_id").alias("_thas"),
    )
    aliases = spark.read.table(f"{SILVER}.seed_team_alias").select(
        F.col("source_team").alias("_atn"),
        F.col("canonical_team_id").alias("_atid"),
    )
    is_tbd = F.upper(F.coalesce(F.col(name_col), F.lit(""))) == "TBD"
    joined = df.join(
        yearly,
        (df["year"] == yearly["_ty"]) & (df[name_col] == yearly["_tn"]) & ~is_tbd,
        how="left",
    ).join(
        aliases,
        (df[name_col] == aliases["_atn"]) & ~is_tbd,
        how="left",
    )
    resolved_id = (
        F.when(F.col(name_col).isNull(), F.lit(None).cast("string"))
        .when(is_tbd, F.lit("TBD"))
        .otherwise(
            F.coalesce(
                F.col("_tid"),
                F.col("_atid"),
                surrogate_key("SK_TEAM", F.col(name_col)),
            )
        )
    )
    out = joined.withColumn(id_out, resolved_id)
    if has_src_out:
        out = out.withColumn(
            has_src_out,
            F.when(is_tbd | F.col(name_col).isNull(), F.lit(False)).otherwise(
                F.coalesce(F.col("_thas"), F.col("_atid").isNotNull(), F.lit(False))
            ),
        )
    return out.drop("_ty", "_tn", "_tid", "_thas", "_atn", "_atid")


def _resolve_player(df: DataFrame, name_col: str, id_out: str, has_src_out: str | None = None) -> DataFrame:
    """Resolve player name -> player_id via yearly ids (1:1 within year), else surrogate key."""
    yearly = spark.read.table(f"{SILVER}.ref_yearly_player_lookup").select(
        F.col("year").alias("_py"),
        F.col("player_name").alias("_pn"),
        F.col("player_id").alias("_pid"),
        F.col("has_source_id").alias("_phas"),
    )
    joined = df.join(
        yearly,
        (df["year"] == yearly["_py"]) & (df[name_col] == yearly["_pn"]),
        how="left",
    )
    out = joined.withColumn(
        id_out,
        F.when(F.col(name_col).isNull(), F.lit(None).cast("string")).otherwise(
            F.coalesce(F.col("_pid"), surrogate_key("SK_PLAYER", F.col("year"), F.col(name_col)))
        ),
    )
    if has_src_out:
        out = out.withColumn(has_src_out, F.coalesce(F.col("_phas"), F.lit(False)))
    return out.drop("_py", "_pn", "_pid", "_phas")


def _resolve_game(df: DataFrame) -> DataFrame:
    """Resolve map-level rows via ref_yearly_game_lookup, excluding ambiguous game keys."""
    ambig = spark.read.table(f"{SILVER}.quarantine_ambiguous_games").select(
        "year", "tournament", "stage", "match_type", "match_name", "map"
    )
    clean_df = df.join(ambig, on=["year", "tournament", "stage", "match_type", "match_name", "map"], how="left_anti")
    games = spark.read.table(f"{SILVER}.ref_yearly_game_lookup")
    return clean_df.join(
        games,
        on=["year", "tournament", "stage", "match_type", "match_name", "map"],
        how="left",
    )


def _resolve_match(df: DataFrame) -> DataFrame:
    """Resolve match-level / All Maps rows via ref_yearly_match_lookup."""
    matches = spark.read.table(f"{SILVER}.ref_yearly_match_lookup")
    return df.join(
        matches,
        on=["year", "tournament", "stage", "match_type", "match_name"],
        how="left",
    )


@dp.materialized_view(
    name=f"{SILVER}.fct_match",
    comment="Match-level scores and outcomes (grain: 1 row per match). winner_team_id is NULL on draws.",
    table_properties={"quality": "silver"},
)
@dp.expect("valid_match_scores", "team_a_score >= 0 AND team_b_score >= 0")
def fct_match():
    src = latest_snapshot_by_year(spark.read.table(f"{BRONZE}.matches_scores")).select(
        F.col("_year").alias("year"),
        F.col("_snapshot_id").alias("_snapshot_id"),
        clean_str("tournament").alias("tournament"),
        clean_str("stage").alias("stage"),
        clean_str("match_type").alias("match_type"),
        clean_str("match_name").alias("match_name"),
        clean_str("team_a").alias("team_a_name"),
        clean_str("team_b").alias("team_b_name"),
        parse_int("team_a_score").alias("team_a_score"),
        parse_int("team_b_score").alias("team_b_score"),
        clean_str("match_result").alias("match_result"),
    )
    df = _resolve_match(_normalize_tournament(src))
    df = _resolve_team(df, "team_a_name", "team_a_id")
    df = _resolve_team(df, "team_b_name", "team_b_id")

    is_draw = (F.lower(F.coalesce(F.col("match_result"), F.lit(""))) == "draw") | (
        F.col("team_a_score").isNotNull() & (F.col("team_a_score") == F.col("team_b_score"))
    )
    winner_team_id = (
        F.when(is_draw, F.lit(None).cast("string"))
        .when(F.col("team_a_score") > F.col("team_b_score"), F.col("team_a_id"))
        .when(F.col("team_b_score") > F.col("team_a_score"), F.col("team_b_id"))
        .otherwise(F.lit(None).cast("string"))
    )
    return (
        df.withColumn("is_draw", is_draw)
        .withColumn("winner_team_id", winner_team_id)
        .withColumn(
            "is_showmatch",
            (F.lower(F.coalesce(F.col("stage"), F.lit(""))) == "showmatch")
            | F.lower(F.coalesce(F.col("match_type"), F.lit(""))).contains("showmatch"),
        )
        .withColumn("documented_gap_reason", documented_gap_reason_expr("tournament"))
    )


@dp.materialized_view(
    name=f"{SILVER}.fct_map_game",
    comment=(
        "Map/game-level scores and durations (grain: 1 row per game). "
        "Derives has_performance_stats and has_economy_stats from data presence."
    ),
    table_properties={"quality": "silver"},
)
@dp.expect("valid_map_scores", "team_a_score >= 0 AND team_b_score >= 0")
def fct_map_game():
    src = latest_snapshot_by_year(spark.read.table(f"{BRONZE}.matches_maps_scores")).select(
        F.col("_year").alias("year"),
        F.col("_snapshot_id").alias("_snapshot_id"),
        clean_str("tournament").alias("tournament"),
        clean_str("stage").alias("stage"),
        clean_str("match_type").alias("match_type"),
        clean_str("match_name").alias("match_name"),
        clean_str("map").alias("map"),
        clean_str("team_a").alias("team_a_name"),
        clean_str("team_b").alias("team_b_name"),
        parse_int("team_a_score").alias("team_a_score"),
        parse_int("team_a_attacker_score").alias("team_a_attacker_score"),
        parse_int("team_a_defender_score").alias("team_a_defender_score"),
        parse_int("team_a_overtime_score").alias("_raw_a_ot"),
        parse_int("team_b_score").alias("team_b_score"),
        parse_int("team_b_attacker_score").alias("team_b_attacker_score"),
        parse_int("team_b_defender_score").alias("team_b_defender_score"),
        parse_int("team_b_overtime_score").alias("_raw_b_ot"),
        parse_duration_sec("duration").alias("duration_seconds"),
    )
    df = _resolve_game(_normalize_tournament(src))
    df = _resolve_team(df, "team_a_name", "team_a_id")
    df = _resolve_team(df, "team_b_name", "team_b_id")

    is_ot = F.col("_raw_a_ot").isNotNull() | F.col("_raw_b_ot").isNotNull()
    df = (
        df.withColumn("is_overtime", is_ot)
        .withColumn("team_a_overtime_score", F.when(is_ot, F.col("_raw_a_ot")).otherwise(F.lit(0)))
        .withColumn("team_b_overtime_score", F.when(is_ot, F.col("_raw_b_ot")).otherwise(F.lit(0)))
        .drop("_raw_a_ot", "_raw_b_ot")
    )

    # Derive data-presence coverage flags from actual silver/bronze fact presence (not from names).
    perf_games = (
        spark.read.table(f"{SILVER}.fct_player_map_performance")
        .filter(F.col("game_id").isNotNull())
        .select("game_id")
        .distinct()
        .withColumn("_has_perf", F.lit(True))
    )
    eco_games = (
        spark.read.table(f"{SILVER}.fct_economy_round")
        .filter(F.col("game_id").isNotNull())
        .select("game_id")
        .distinct()
        .withColumn("_has_eco", F.lit(True))
    )

    return (
        df.join(perf_games, on="game_id", how="left")
        .join(eco_games, on="game_id", how="left")
        .withColumn("has_performance_stats", F.coalesce(F.col("_has_perf"), F.lit(False)))
        .withColumn("has_economy_stats", F.coalesce(F.col("_has_eco"), F.lit(False)))
        .drop("_has_perf", "_has_eco")
        .withColumn("is_draw", F.col("team_a_score").isNotNull() & (F.col("team_a_score") == F.col("team_b_score")))
        .withColumn(
            "winner_team_id",
            F.when(F.col("team_a_score") > F.col("team_b_score"), F.col("team_a_id"))
            .when(F.col("team_b_score") > F.col("team_a_score"), F.col("team_b_id"))
            .otherwise(F.lit(None).cast("string")),
        )
        .withColumn(
            "is_showmatch",
            (F.lower(F.coalesce(F.col("stage"), F.lit(""))) == "showmatch")
            | F.lower(F.coalesce(F.col("match_type"), F.lit(""))).contains("showmatch"),
        )
        .withColumn("documented_gap_reason", documented_gap_reason_expr("tournament"))
    )


def _base_overview_df() -> DataFrame:
    return latest_snapshot_by_year(spark.read.table(f"{BRONZE}.matches_overview")).select(
        F.col("_year").alias("year"),
        F.col("_snapshot_id").alias("_snapshot_id"),
        clean_str("tournament").alias("tournament"),
        clean_str("stage").alias("stage"),
        clean_str("match_type").alias("match_type"),
        clean_str("match_name").alias("match_name"),
        clean_str("map").alias("map"),
        clean_str("player").alias("player_name"),
        clean_str("team").alias("team_name"),
        F.lower(clean_str("agents")).alias("agent"),
        F.lower(clean_str("side")).alias("side"),
        parse_double("rating").alias("rating"),
        parse_int("average_combat_score").alias("acs"),
        parse_int("kills").alias("kills"),
        parse_int("deaths").alias("deaths"),
        parse_int("assists").alias("assists"),
        parse_int("kills_deaths_kd").alias("kd_diff"),
        parse_pct("kill_assist_trade_survive_pct").alias("kast_pct"),
        parse_int("average_damage_per_round").alias("adr"),
        parse_pct("headshot_pct").alias("hs_pct"),
        parse_int("first_kills").alias("first_kills"),
        parse_int("first_deaths").alias("first_deaths"),
        parse_int("kills_deaths_fkd").alias("fk_fd_diff"),
    )


@dp.materialized_view(
    name=f"{SILVER}.fct_player_map_stats",
    comment="Player stats at map + side grain (excludes 'All Maps' rollups; no zero-fill).",
    table_properties={"quality": "silver"},
)
@dp.expect_all(
    {
        "non_negative_kills": "kills IS NULL OR kills >= 0",
        "non_negative_deaths": "deaths IS NULL OR deaths >= 0",
        "valid_hs_pct": "hs_pct IS NULL OR (hs_pct >= 0.0 AND hs_pct <= 1.0)",
        "valid_kast_pct": "kast_pct IS NULL OR (kast_pct >= 0.0 AND kast_pct <= 1.0)",
    }
)
def fct_player_map_stats():
    src = _base_overview_df().filter(F.col("map") != "All Maps")
    df = _resolve_game(_normalize_tournament(src))
    df = _resolve_team(df, "team_name", "team_id", "team_has_source_id")
    df = _resolve_player(df, "player_name", "player_id", "player_has_source_id")
    return (
        df.withColumn("is_map_rollup", F.lit(False))
        .withColumn(
            "is_showmatch",
            (F.lower(F.coalesce(F.col("stage"), F.lit(""))) == "showmatch")
            | F.lower(F.coalesce(F.col("match_type"), F.lit(""))).contains("showmatch"),
        )
        .withColumn("documented_gap_reason", documented_gap_reason_expr("tournament"))
    )


@dp.materialized_view(
    name=f"{SILVER}.fct_player_match_rollup_stats",
    comment="Separated 'All Maps' match-level rollup rows from matches_overview (is_map_rollup=true).",
    table_properties={"quality": "silver"},
)
def fct_player_match_rollup_stats():
    src = _base_overview_df().filter(F.col("map") == "All Maps")
    df = _resolve_match(_normalize_tournament(src))
    df = _resolve_team(df, "team_name", "team_id", "team_has_source_id")
    df = _resolve_player(df, "player_name", "player_id", "player_has_source_id")
    return df.withColumn("is_map_rollup", F.lit(True)).withColumn(
        "documented_gap_reason", documented_gap_reason_expr("tournament")
    )


@dp.materialized_view(
    name=f"{SILVER}.fct_player_map_performance",
    comment="Player multi-kill, clutch, econ rating, and spike stats at map grain (excludes 'All Maps' rollups).",
    table_properties={"quality": "silver"},
)
def fct_player_map_performance():
    src = (
        latest_snapshot_by_year(spark.read.table(f"{BRONZE}.matches_kills_stats"))
        .select(
            F.col("_year").alias("year"),
            F.col("_snapshot_id").alias("_snapshot_id"),
            clean_str("tournament").alias("tournament"),
            clean_str("stage").alias("stage"),
            clean_str("match_type").alias("match_type"),
            clean_str("match_name").alias("match_name"),
            clean_str("map").alias("map"),
            clean_str("player").alias("player_name"),
            clean_str("team").alias("team_name"),
            F.lower(clean_str("agents")).alias("agent"),
            parse_int("2k").alias("kills_2k"),
            parse_int("3k").alias("kills_3k"),
            parse_int("4k").alias("kills_4k"),
            parse_int("5k").alias("kills_5k"),
            parse_int("1v1").alias("clutch_1v1"),
            parse_int("1v2").alias("clutch_1v2"),
            parse_int("1v3").alias("clutch_1v3"),
            parse_int("1v4").alias("clutch_1v4"),
            parse_int("1v5").alias("clutch_1v5"),
            parse_int("econ").alias("econ_rating"),
            parse_int("spike_plants").alias("spike_plants"),
            parse_int("spike_defuses").alias("spike_defuses"),
        )
        .filter(F.col("map") != "All Maps")
    )
    df = _resolve_game(_normalize_tournament(src))
    df = _resolve_team(df, "team_name", "team_id")
    df = _resolve_player(df, "player_name", "player_id")
    return df.withColumn("is_map_rollup", F.lit(False)).withColumn(
        "documented_gap_reason", documented_gap_reason_expr("tournament")
    )


@dp.materialized_view(
    name=f"{SILVER}.fct_economy_round",
    comment="Round-level team economy and loadout values (1 row per game-round-team; 2025 aliases normalized).",
    table_properties={"quality": "silver"},
)
@dp.expect("valid_round_number", "round_number IS NULL OR round_number >= 1")
def fct_economy_round():
    src = latest_snapshot_by_year(spark.read.table(f"{BRONZE}.matches_eco_rounds")).select(
        F.col("_year").alias("year"),
        F.col("_snapshot_id").alias("_snapshot_id"),
        clean_str("tournament").alias("tournament"),
        clean_str("stage").alias("stage"),
        clean_str("match_type").alias("match_type"),
        clean_str("match_name").alias("match_name"),
        clean_str("map").alias("map"),
        parse_int("round_number").alias("round_number"),
        clean_str("team").alias("team_name"),
        parse_credits("loadout_value").alias("loadout_value_credits"),
        parse_credits("remaining_credits").alias("remaining_credits"),
        clean_str("type").alias("buy_type"),
        clean_str("outcome").alias("round_outcome"),
    )
    df = _resolve_game(_normalize_tournament(src))
    df = _resolve_team(df, "team_name", "team_id")
    return df.withColumn("documented_gap_reason", documented_gap_reason_expr("tournament"))


@dp.materialized_view(
    name=f"{SILVER}.fct_economy_summary",
    comment="Wide-format team economy summary per map (pivoted from 5 long rows; excludes 'All Maps').",
    table_properties={"quality": "silver"},
)
def fct_economy_summary():
    src = (
        latest_snapshot_by_year(spark.read.table(f"{BRONZE}.matches_eco_stats"))
        .select(
            F.col("_year").alias("year"),
            F.col("_snapshot_id").alias("_snapshot_id"),
            clean_str("tournament").alias("tournament"),
            clean_str("stage").alias("stage"),
            clean_str("match_type").alias("match_type"),
            clean_str("match_name").alias("match_name"),
            clean_str("map").alias("map"),
            clean_str("team").alias("team_name"),
            clean_str("type").alias("eco_type"),
            parse_int("initiated").alias("initiated"),
            parse_int("won").alias("won"),
        )
        .filter(F.col("map") != "All Maps")
    )
    df = _resolve_game(_normalize_tournament(src))
    df = _resolve_team(df, "team_name", "team_id")

    group_cols = [
        "year",
        "_snapshot_id",
        "tournament_id",
        "stage_id",
        "match_type_id",
        "match_id",
        "game_id",
        "tournament",
        "stage",
        "match_type",
        "match_name",
        "map",
        "team_id",
        "team_name",
    ]
    return (
        df.groupBy(*group_cols)
        .agg(
            F.max(F.when(F.col("eco_type") == "Pistol Won", F.col("won"))).alias("pistol_won"),
            F.max(F.when(F.col("eco_type") == "Eco (won)", F.col("initiated"))).alias("eco_initiated"),
            F.max(F.when(F.col("eco_type") == "Eco (won)", F.col("won"))).alias("eco_won"),
            F.max(F.when(F.col("eco_type") == "$ (won)", F.col("initiated"))).alias("semi_eco_initiated"),
            F.max(F.when(F.col("eco_type") == "$ (won)", F.col("won"))).alias("semi_eco_won"),
            F.max(F.when(F.col("eco_type") == "$$ (won)", F.col("initiated"))).alias("semi_buy_initiated"),
            F.max(F.when(F.col("eco_type") == "$$ (won)", F.col("won"))).alias("semi_buy_won"),
            F.max(F.when(F.col("eco_type") == "$$$ (won)", F.col("initiated"))).alias("full_buy_initiated"),
            F.max(F.when(F.col("eco_type") == "$$$ (won)", F.col("won"))).alias("full_buy_won"),
        )
        .withColumn("is_map_rollup", F.lit(False))
        .withColumn("documented_gap_reason", documented_gap_reason_expr("tournament"))
    )


@dp.materialized_view(
    name=f"{SILVER}.fct_kill_pair",
    comment=(
        "Player-pair head-to-head kills pivoted across All/First/Op kill types "
        "(grain: 1 row per game-player-enemy; excludes 'All Maps')."
    ),
    table_properties={"quality": "silver"},
)
def fct_kill_pair():
    src = (
        latest_snapshot_by_year(spark.read.table(f"{BRONZE}.matches_kills"))
        .select(
            F.col("_year").alias("year"),
            F.col("_snapshot_id").alias("_snapshot_id"),
            clean_str("tournament").alias("tournament"),
            clean_str("stage").alias("stage"),
            clean_str("match_type").alias("match_type"),
            clean_str("match_name").alias("match_name"),
            clean_str("map").alias("map"),
            clean_str("player").alias("player_name"),
            clean_str("player_team").alias("player_team_name"),
            clean_str("enemy").alias("enemy_name"),
            clean_str("enemy_team").alias("enemy_team_name"),
            clean_str("kill_type").alias("kill_type"),
            parse_int("player_kills").alias("player_kills"),
            parse_int("enemy_kills").alias("enemy_kills"),
            parse_int("difference").alias("kill_diff"),
        )
        .filter(F.col("map") != "All Maps")
    )
    df = _resolve_game(_normalize_tournament(src))
    df = _resolve_team(df, "player_team_name", "player_team_id")
    df = _resolve_team(df, "enemy_team_name", "enemy_team_id")
    df = _resolve_player(df, "player_name", "player_id")
    df = _resolve_player(df, "enemy_name", "enemy_id")

    group_cols = [
        "year",
        "_snapshot_id",
        "tournament_id",
        "match_id",
        "game_id",
        "tournament",
        "map",
        "player_id",
        "player_name",
        "player_team_id",
        "enemy_id",
        "enemy_name",
        "enemy_team_id",
    ]
    return (
        df.groupBy(*group_cols)
        .agg(
            F.max(F.when(F.col("kill_type") == "All Kills", F.col("player_kills"))).alias("all_player_kills"),
            F.max(F.when(F.col("kill_type") == "All Kills", F.col("enemy_kills"))).alias("all_enemy_kills"),
            F.max(F.when(F.col("kill_type") == "All Kills", F.col("kill_diff"))).alias("all_kill_diff"),
            F.max(F.when(F.col("kill_type") == "First Kills", F.col("player_kills"))).alias("first_player_kills"),
            F.max(F.when(F.col("kill_type") == "First Kills", F.col("enemy_kills"))).alias("first_enemy_kills"),
            F.max(F.when(F.col("kill_type") == "First Kills", F.col("kill_diff"))).alias("first_kill_diff"),
            F.max(F.when(F.col("kill_type") == "Op Kills", F.col("player_kills"))).alias("op_player_kills"),
            F.max(F.when(F.col("kill_type") == "Op Kills", F.col("enemy_kills"))).alias("op_enemy_kills"),
            F.max(F.when(F.col("kill_type") == "Op Kills", F.col("kill_diff"))).alias("op_kill_diff"),
        )
        .withColumn("is_map_rollup", F.lit(False))
    )


@dp.materialized_view(
    name=f"{SILVER}.fct_round_outcome",
    comment="Round-by-round win/loss outcomes with canonical 4-value method enum (grain: 1 row per game-round-team).",
    table_properties={"quality": "silver"},
)
@dp.expect(
    "valid_canonical_method",
    "win_loss_method IN ('ELIMINATION', 'SPIKE_DEFUSED', 'SPIKE_DETONATED', 'TIME_EXPIRY')",
)
def fct_round_outcome():
    src = latest_snapshot_by_year(spark.read.table(f"{BRONZE}.matches_win_loss_methods_round_number")).select(
        F.col("_year").alias("year"),
        F.col("_snapshot_id").alias("_snapshot_id"),
        clean_str("tournament").alias("tournament"),
        clean_str("stage").alias("stage"),
        clean_str("match_type").alias("match_type"),
        clean_str("match_name").alias("match_name"),
        clean_str("map").alias("map"),
        parse_int("round_number").alias("round_number"),
        clean_str("team").alias("team_name"),
        clean_str("outcome").alias("outcome"),
        clean_str("method").alias("raw_method"),
    )
    df = _resolve_game(_normalize_tournament(src))
    df = _resolve_team(df, "team_name", "team_id")
    return df.withColumn("win_loss_method", canonical_win_loss_method_expr("raw_method"))


@dp.materialized_view(
    name=f"{SILVER}.fct_draft",
    comment="Match map veto (ban/pick/remains) actions at match grain.",
    table_properties={"quality": "silver"},
)
def fct_draft():
    src = latest_snapshot_by_year(spark.read.table(f"{BRONZE}.matches_draft_phase")).select(
        F.col("_year").alias("year"),
        F.col("_snapshot_id").alias("_snapshot_id"),
        clean_str("tournament").alias("tournament"),
        clean_str("stage").alias("stage"),
        clean_str("match_type").alias("match_type"),
        clean_str("match_name").alias("match_name"),
        clean_str("team").alias("team_name"),
        F.lower(clean_str("action")).alias("action"),
        clean_str("map").alias("map_name"),
    )
    df = _resolve_match(_normalize_tournament(src))
    return _resolve_team(df, "team_name", "team_id")


@dp.materialized_view(
    name=f"{SILVER}.fct_highlight_round_kill",
    comment=(
        "Highlight round kill events from matches_rounds_kills. VERIFIED: contains ONLY multi-kill (2k-5k) "
        "and clutch (1v1-1v5) tagged kills, NOT all match kills. Do NOT sum as total player kills."
    ),
    table_properties={"quality": "silver"},
)
def fct_highlight_round_kill():
    src = latest_snapshot_by_year(spark.read.table(f"{BRONZE}.matches_rounds_kills")).select(
        F.col("_year").alias("year"),
        F.col("_snapshot_id").alias("_snapshot_id"),
        clean_str("tournament").alias("tournament"),
        clean_str("stage").alias("stage"),
        clean_str("match_type").alias("match_type"),
        clean_str("match_name").alias("match_name"),
        clean_str("map").alias("map"),
        parse_int("round_number").alias("round_number"),
        clean_str("eliminator").alias("eliminator_name"),
        clean_str("eliminator_team").alias("eliminator_team_name"),
        F.lower(clean_str("eliminator_agent")).alias("eliminator_agent"),
        clean_str("eliminated").alias("eliminated_name"),
        clean_str("eliminated_team").alias("eliminated_team_name"),
        F.lower(clean_str("eliminated_agent")).alias("eliminated_agent"),
        clean_str("kill_type").alias("highlight_tag"),
    )
    df = _resolve_game(_normalize_tournament(src))
    df = _resolve_team(df, "eliminator_team_name", "eliminator_team_id")
    df = _resolve_team(df, "eliminated_team_name", "eliminated_team_id")
    df = _resolve_player(df, "eliminator_name", "eliminator_player_id")
    df = _resolve_player(df, "eliminated_name", "eliminated_player_id")
    return df
