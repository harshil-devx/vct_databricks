"""Silver conformed dimensions, yearly lookup helpers, and ambiguous-game quarantine.

Key rules enforced here:
- Use all_ids_tournament_stage_match_types as the match-type dimension source (superset of yearly files).
- Within a single year, player/team names map 1:1 to IDs via yearly ids_* tables; across years
  we conform on ID and retain name history arrays.
- Ambiguous game keys (19 in 2021, 4 in 2022) that map to >1 Game ID are isolated in
  quarantine_ambiguous_games so fact joins never fan out.
- Placeholder team 'TBD' is flagged (is_placeholder_tbd = true) and excluded from team joins.
- ~410 ID-less 2021 players receive deterministic surrogate keys with has_source_id = false.
"""

from __future__ import annotations

from pyspark import pipelines as dp
from pyspark.sql import Window
from pyspark.sql import functions as F

from pipelines.silver.silver_transforms import (
    clean_str,
    documented_gap_reason_expr,
    latest_snapshot_by_year,
    latest_snapshot_global,
    parse_int,
    surrogate_key,
)

BRONZE = spark.conf.get("vct.bronze_schema")
SILVER = spark.conf.get("vct.silver_schema")


@dp.materialized_view(
    name=f"{SILVER}.dim_tournament",
    comment="Conformed tournament dimension from all_ids, classified by tournament_tier and documented_gap_reason.",
    table_properties={"quality": "silver"},
)
@dp.expect_or_fail("tournament_id_not_null", "tournament_id IS NOT NULL")
def dim_tournament():
    src = latest_snapshot_global(spark.read.table(f"{BRONZE}.all_ids_tournament_stage_match_types"))
    base = (
        src.select(
            clean_str("tournament_id").alias("tournament_id"),
            clean_str("tournament").alias("tournament_name"),
            parse_int("year").alias("year"),
        )
        .filter(F.col("tournament_id").isNotNull())
        .dropDuplicates(["tournament_id"])
    )

    tiers = spark.read.table(f"{SILVER}.seed_tournament_tier")
    matched = base.join(
        tiers,
        F.expr("tournament_name RLIKE regex_pattern"),
        how="left",
    )
    w = Window.partitionBy("tournament_id").orderBy(F.col("pattern_order").asc_nulls_last())
    return (
        matched.withColumn("_rn", F.row_number().over(w))
        .filter(F.col("_rn") == 1)
        .select(
            "tournament_id",
            "tournament_name",
            "year",
            F.coalesce(F.col("tournament_tier"), F.lit("Other")).alias("tournament_tier"),
            documented_gap_reason_expr("tournament_name").alias("documented_gap_reason"),
        )
    )


@dp.materialized_view(
    name=f"{SILVER}.dim_stage_match_type",
    comment="Conformed tournament/stage/match-type dimension from all_ids (superset of yearly ids).",
    table_properties={"quality": "silver"},
)
@dp.expect_or_fail("match_type_id_not_null", "match_type_id IS NOT NULL")
def dim_stage_match_type():
    src = latest_snapshot_global(spark.read.table(f"{BRONZE}.all_ids_tournament_stage_match_types"))
    return (
        src.select(
            clean_str("tournament_id").alias("tournament_id"),
            clean_str("tournament").alias("tournament_name"),
            clean_str("stage_id").alias("stage_id"),
            clean_str("stage").alias("stage_name"),
            clean_str("match_type_id").alias("match_type_id"),
            clean_str("match_type").alias("match_type_name"),
            parse_int("year").alias("year"),
        )
        .filter(F.col("match_type_id").isNotNull())
        .withColumn(
            "is_showmatch",
            (F.lower(F.coalesce(F.col("stage_name"), F.lit(""))) == "showmatch")
            | F.lower(F.coalesce(F.col("match_type_name"), F.lit(""))).contains("showmatch"),
        )
        .dropDuplicates(["tournament_id", "stage_id", "match_type_id"])
    )


@dp.materialized_view(
    name=f"{SILVER}.ref_yearly_team_lookup",
    comment="Yearly team_name -> team_id lookup (1:1 within year) plus team_alias fallback and surrogate keys.",
    table_properties={"quality": "silver"},
)
def ref_yearly_team_lookup():
    yearly = latest_snapshot_by_year(spark.read.table(f"{BRONZE}.ids_teams")).select(
        F.col("_year").alias("year"),
        clean_str("team").alias("team_name"),
        clean_str("team_id").alias("source_team_id"),
    )
    return (
        yearly.filter(F.col("team_name").isNotNull())
        .withColumn("is_placeholder_tbd", F.upper(F.col("team_name")) == "TBD")
        .withColumn("has_source_id", F.col("source_team_id").isNotNull() & ~F.col("is_placeholder_tbd"))
        .withColumn(
            "team_id",
            F.when(F.col("is_placeholder_tbd"), F.lit("TBD"))
            .when(F.col("source_team_id").isNotNull(), F.col("source_team_id"))
            .otherwise(surrogate_key("SK_TEAM", F.col("team_name"))),
        )
        .dropDuplicates(["year", "team_name"])
    )


@dp.materialized_view(
    name=f"{SILVER}.dim_team",
    comment="Conformed team dimension keyed by team_id with cross-year name history and abbreviations.",
    table_properties={"quality": "silver"},
)
@dp.expect_or_fail("team_id_not_null", "team_id IS NOT NULL")
def dim_team():
    yearly = spark.read.table(f"{SILVER}.ref_yearly_team_lookup").select(
        "team_id",
        "team_name",
        "year",
        "has_source_id",
        "is_placeholder_tbd",
    )
    all_teams = (
        latest_snapshot_global(spark.read.table(f"{BRONZE}.all_ids_teams"))
        .select(
            clean_str("team_id").alias("team_id"),
            clean_str("team").alias("team_name"),
            F.lit(None).cast("int").alias("year"),
            F.lit(True).alias("has_source_id"),
            (F.upper(F.coalesce(clean_str("team"), F.lit(""))) == "TBD").alias("is_placeholder_tbd"),
        )
        .filter(F.col("team_id").isNotNull() & F.col("team_name").isNotNull())
    )
    aliases = (
        spark.read.table(f"{SILVER}.seed_team_alias")
        .select(
            F.coalesce(
                clean_str("canonical_team_id"),
                surrogate_key("SK_TEAM", clean_str("source_team")),
            ).alias("team_id"),
            clean_str("canonical_team_name").alias("team_name"),
            F.lit(2026).alias("year"),
            clean_str("canonical_team_id").isNotNull().alias("has_source_id"),
            F.lit(False).alias("is_placeholder_tbd"),
        )
    )

    combined = yearly.unionByName(all_teams).unionByName(aliases)
    latest_name_w = Window.partitionBy("team_id").orderBy(F.col("year").desc_nulls_last(), F.col("team_name").asc())
    canonical = (
        combined.withColumn("_rn", F.row_number().over(latest_name_w))
        .filter(F.col("_rn") == 1)
        .select("team_id", F.col("team_name").alias("canonical_team_name"))
    )

    history = combined.groupBy("team_id").agg(
        F.array_sort(F.collect_set("team_name")).alias("team_name_history"),
        F.max("has_source_id").alias("has_source_id"),
        F.max("is_placeholder_tbd").alias("is_placeholder_tbd"),
        F.min("year").alias("first_seen_year"),
        F.max("year").alias("last_seen_year"),
    )

    abbrevs = (
        latest_snapshot_global(spark.read.table(f"{BRONZE}.all_ids_teams_mapping"))
        .select(
            clean_str("full_name").alias("canonical_team_name"),
            clean_str("abbreviated").alias("abbreviated"),
        )
        .filter(F.col("canonical_team_name").isNotNull() & F.col("abbreviated").isNotNull())
        .groupBy("canonical_team_name")
        .agg(F.array_sort(F.collect_set("abbreviated")).alias("abbreviated_names"))
    )

    return (
        canonical.join(history, on="team_id", how="inner")
        .join(abbrevs, on="canonical_team_name", how="left")
        .select(
            "team_id",
            "canonical_team_name",
            "team_name_history",
            "abbreviated_names",
            "has_source_id",
            "is_placeholder_tbd",
            "first_seen_year",
            "last_seen_year",
        )
    )


@dp.materialized_view(
    name=f"{SILVER}.ref_yearly_player_lookup",
    comment="Yearly player_name -> player_id lookup (1:1 within year) with surrogate keys for ID-less players.",
    table_properties={"quality": "silver"},
)
def ref_yearly_player_lookup():
    yearly = latest_snapshot_by_year(spark.read.table(f"{BRONZE}.ids_players")).select(
        F.col("_year").alias("year"),
        clean_str("player").alias("player_name"),
        clean_str("player_id").alias("source_player_id"),
    )
    return (
        yearly.filter(F.col("player_name").isNotNull())
        .withColumn("has_source_id", F.col("source_player_id").isNotNull())
        .withColumn(
            "player_id",
            F.coalesce(
                F.col("source_player_id"),
                surrogate_key("SK_PLAYER", F.col("year"), F.col("player_name")),
            ),
        )
        .dropDuplicates(["year", "player_name"])
    )


@dp.materialized_view(
    name=f"{SILVER}.dim_player",
    comment="Conformed player dimension keyed by player_id with name history; preserves literal 'nan'.",
    table_properties={"quality": "silver"},
)
@dp.expect_or_fail("player_id_not_null", "player_id IS NOT NULL")
def dim_player():
    yearly = spark.read.table(f"{SILVER}.ref_yearly_player_lookup").select(
        "player_id",
        "player_name",
        "year",
        "has_source_id",
    )
    all_players = (
        latest_snapshot_global(spark.read.table(f"{BRONZE}.all_ids_players"))
        .select(
            clean_str("player_id").alias("player_id"),
            clean_str("player").alias("player_name"),
            F.lit(None).cast("int").alias("year"),
            F.lit(True).alias("has_source_id"),
        )
        .filter(F.col("player_id").isNotNull() & F.col("player_name").isNotNull())
    )

    combined = yearly.unionByName(all_players)
    latest_name_w = Window.partitionBy("player_id").orderBy(F.col("year").desc_nulls_last(), F.col("player_name").asc())
    canonical = (
        combined.withColumn("_rn", F.row_number().over(latest_name_w))
        .filter(F.col("_rn") == 1)
        .select("player_id", F.col("player_name").alias("canonical_player_name"))
    )

    history = combined.groupBy("player_id").agg(
        F.array_sort(F.collect_set("player_name")).alias("player_name_history"),
        F.max("has_source_id").alias("has_source_id"),
        F.min("year").alias("first_seen_year"),
        F.max("year").alias("last_seen_year"),
    )

    return canonical.join(history, on="player_id", how="inner").select(
        "player_id",
        "canonical_player_name",
        "player_name_history",
        "has_source_id",
        "first_seen_year",
        "last_seen_year",
    )


@dp.materialized_view(
    name=f"{SILVER}.dim_agent",
    comment="Conformed agent dimension combining seed_agent_release with observed first_seen_year.",
    table_properties={"quality": "silver"},
)
@dp.expect_or_fail("agent_not_null", "agent IS NOT NULL")
def dim_agent():
    return spark.read.table(f"{SILVER}.seed_agent_release").select(
        F.lower(F.trim(F.col("agent"))).alias("agent"),
        "first_seen_year",
        "release_date",
        "release_date_verified",
        "notes",
    )


@dp.materialized_view(
    name=f"{SILVER}.dim_map",
    comment="Conformed Valorant map dimension (excluding 'All Maps' rollup placeholder).",
    table_properties={"quality": "silver"},
)
@dp.expect_or_fail("map_name_not_null", "map_name IS NOT NULL")
def dim_map():
    maps_scores = latest_snapshot_by_year(spark.read.table(f"{BRONZE}.matches_maps_scores")).select(
        clean_str("map").alias("map_name"),
        F.col("_year").alias("year"),
    )
    agents_maps = latest_snapshot_by_year(spark.read.table(f"{BRONZE}.agents_maps_stats")).select(
        clean_str("map").alias("map_name"),
        F.col("_year").alias("year"),
    )
    return (
        maps_scores.unionByName(agents_maps)
        .filter(F.col("map_name").isNotNull() & (F.col("map_name") != "All Maps"))
        .groupBy("map_name")
        .agg(
            F.min("year").alias("first_seen_year"),
            F.max("year").alias("last_seen_year"),
        )
    )


@dp.materialized_view(
    name=f"{SILVER}.ref_yearly_game_lookup",
    comment="Unambiguous yearly (tournament, stage, match_type, match_name, map) -> game_id/match_id lookup.",
    table_properties={"quality": "silver"},
)
def ref_yearly_game_lookup():
    src = latest_snapshot_by_year(spark.read.table(f"{BRONZE}.ids_tournament_stage_matches_games")).select(
        F.col("_year").alias("year"),
        clean_str("tournament").alias("tournament"),
        clean_str("stage").alias("stage"),
        clean_str("match_type").alias("match_type"),
        clean_str("match_name").alias("match_name"),
        clean_str("map").alias("map"),
        clean_str("tournament_id").alias("tournament_id"),
        clean_str("stage_id").alias("stage_id"),
        clean_str("match_id").alias("match_id"),
        clean_str("game_id").alias("game_id"),
    )
    mt = (
        spark.read.table(f"{SILVER}.dim_stage_match_type")
        .select(
            F.col("tournament_id").alias("_mt_tid"),
            F.col("stage_id").alias("_mt_sid"),
            F.col("match_type_name").alias("_mt_name"),
            F.col("match_type_id").alias("match_type_id"),
        )
        .dropDuplicates(["_mt_tid", "_mt_sid", "_mt_name"])
    )
    key_cols = ["year", "tournament", "stage", "match_type", "match_name", "map"]
    counts = src.groupBy(*key_cols).agg(F.countDistinct("game_id").alias("_game_id_cnt"))
    return (
        src.join(counts, on=key_cols, how="inner")
        .filter(F.col("_game_id_cnt") == 1)
        .drop("_game_id_cnt")
        .dropDuplicates(key_cols)
        .join(
            mt,
            (F.col("tournament_id") == mt["_mt_tid"])
            & (F.col("stage_id") == mt["_mt_sid"])
            & (F.col("match_type") == mt["_mt_name"]),
            how="left",
        )
        .drop("_mt_tid", "_mt_sid", "_mt_name")
    )


@dp.materialized_view(
    name=f"{SILVER}.ref_yearly_match_lookup",
    comment="Unambiguous yearly (tournament, stage, match_type, match_name) -> match_id lookup.",
    table_properties={"quality": "silver"},
)
def ref_yearly_match_lookup():
    src = latest_snapshot_by_year(spark.read.table(f"{BRONZE}.ids_tournament_stage_matches_games")).select(
        F.col("_year").alias("year"),
        clean_str("tournament").alias("tournament"),
        clean_str("stage").alias("stage"),
        clean_str("match_type").alias("match_type"),
        clean_str("match_name").alias("match_name"),
        clean_str("tournament_id").alias("tournament_id"),
        clean_str("stage_id").alias("stage_id"),
        clean_str("match_id").alias("match_id"),
    )
    mt = (
        spark.read.table(f"{SILVER}.dim_stage_match_type")
        .select(
            F.col("tournament_id").alias("_mt_tid"),
            F.col("stage_id").alias("_mt_sid"),
            F.col("match_type_name").alias("_mt_name"),
            F.col("match_type_id").alias("match_type_id"),
        )
        .dropDuplicates(["_mt_tid", "_mt_sid", "_mt_name"])
    )
    key_cols = ["year", "tournament", "stage", "match_type", "match_name"]
    counts = src.groupBy(*key_cols).agg(F.countDistinct("match_id").alias("_match_id_cnt"))
    return (
        src.join(counts, on=key_cols, how="inner")
        .filter(F.col("_match_id_cnt") == 1)
        .drop("_match_id_cnt")
        .dropDuplicates(key_cols)
        .join(
            mt,
            (F.col("tournament_id") == mt["_mt_tid"])
            & (F.col("stage_id") == mt["_mt_sid"])
            & (F.col("match_type") == mt["_mt_name"]),
            how="left",
        )
        .drop("_mt_tid", "_mt_sid", "_mt_name")
    )


@dp.materialized_view(
    name=f"{SILVER}.quarantine_ambiguous_games",
    comment="Game keys mapping to >1 Game ID in yearly ids_tournament_stage_matches_games.",
    table_properties={"quality": "silver", "layer": "quarantine"},
)
def quarantine_ambiguous_games():
    src = latest_snapshot_by_year(spark.read.table(f"{BRONZE}.ids_tournament_stage_matches_games")).select(
        F.col("_year").alias("year"),
        F.col("_snapshot_id").alias("_snapshot_id"),
        clean_str("tournament").alias("tournament"),
        clean_str("stage").alias("stage"),
        clean_str("match_type").alias("match_type"),
        clean_str("match_name").alias("match_name"),
        clean_str("map").alias("map"),
        clean_str("match_id").alias("match_id"),
        clean_str("game_id").alias("game_id"),
    )
    key_cols = ["year", "tournament", "stage", "match_type", "match_name", "map"]
    return (
        src.groupBy(*key_cols)
        .agg(
            F.countDistinct("game_id").alias("distinct_game_id_count"),
            F.array_sort(F.collect_set("game_id")).alias("conflicting_game_ids"),
            F.array_sort(F.collect_set("match_id")).alias("conflicting_match_ids"),
            F.max("_snapshot_id").alias("_snapshot_id"),
        )
        .filter(F.col("distinct_game_id_count") > 1)
        .withColumn("quarantine_reason", F.lit("AMBIGUOUS_GAME_KEY_MULTIPLE_GAME_IDS"))
    )
