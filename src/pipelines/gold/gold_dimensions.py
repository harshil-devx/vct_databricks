"""Gold conformed dimensions (star schema) with full table and column comments."""

from __future__ import annotations

from pyspark import pipelines as dp
from pyspark.sql import functions as F

from pipelines.gold.gold_utils import col_doc

SILVER = spark.conf.get("vct.silver_schema")
GOLD = spark.conf.get("vct.gold_schema")


@dp.materialized_view(
    name=f"{GOLD}.dim_tournament",
    comment="Gold tournament dimension with analytical tournament_tier and provider-documented gap flags.",
    table_properties={"quality": "gold"},
)
def gold_dim_tournament():
    df = spark.read.table(f"{SILVER}.dim_tournament")
    return df.select(
        col_doc("tournament_id", "tournament_id", "Unique VLR tournament identifier (PK)."),
        col_doc("tournament_name", "tournament_name", "Canonical tournament name."),
        col_doc("year", "year", "VCT season year (2021-2026)."),
        col_doc(
            "tournament_tier",
            "tournament_tier",
            "Analytical tier (International_Champions, International_Masters, Regional_League, Regional_LCQ, Challengers_Qualifier, Other).",
        ),
        col_doc(
            "documented_gap_reason",
            "documented_gap_reason",
            "Provider-documented stats gap reason (e.g. China-hosted match), NULL if none.",
        ),
    )


@dp.materialized_view(
    name=f"{GOLD}.dim_team",
    comment="Gold conformed team dimension keyed by team_id with name history, abbreviations, and alias status.",
    table_properties={"quality": "gold"},
)
def gold_dim_team():
    teams = spark.read.table(f"{SILVER}.dim_team")
    aliases = (
        spark.read.table(f"{SILVER}.seed_team_alias")
        .select(
            F.col("canonical_team_name").alias("_aname"),
            F.col("status").alias("alias_status"),
        )
        .dropDuplicates(["_aname"])
    )
    return teams.join(aliases, teams["canonical_team_name"] == aliases["_aname"], how="left").select(
        col_doc(
            "team_id",
            "team_id",
            "Conformed team identifier (source Team ID, 'TBD', or deterministic surrogate SK_TEAM_*).",
        ),
        col_doc("canonical_team_name", "canonical_team_name", "Most recent observed team name for this team_id."),
        col_doc(
            "team_name_history", "team_name_history", "Sorted array of all historical names observed for this team_id."
        ),
        col_doc(
            "abbreviated_names",
            "abbreviated_names",
            "Sorted array of known team abbreviations from all_ids_teams_mapping.",
        ),
        col_doc(
            "has_source_id", "has_source_id", "True if backed by a source VLR Team ID; False for surrogate keys or TBD."
        ),
        col_doc("is_placeholder_tbd", "is_placeholder_tbd", "True for the placeholder team 'TBD'."),
        col_doc(
            F.coalesce(F.col("alias_status"), F.lit("verified")),
            "alias_status",
            "Identity status: verified, unverified (e.g. Mega Minors), or unresolved (e.g. Kiwoom DRX).",
        ),
        col_doc("first_seen_year", "first_seen_year", "First VCT season year this team appeared."),
        col_doc("last_seen_year", "last_seen_year", "Most recent VCT season year this team appeared."),
    )


@dp.materialized_view(
    name=f"{GOLD}.dim_player",
    comment="Gold conformed player dimension keyed by player_id with cross-year name history (preserves literal 'nan').",
    table_properties={"quality": "gold"},
)
def gold_dim_player():
    df = spark.read.table(f"{SILVER}.dim_player")
    return df.select(
        col_doc(
            "player_id",
            "player_id",
            "Conformed player identifier (source Player ID or deterministic surrogate SK_PLAYER_*).",
        ),
        col_doc(
            "canonical_player_name",
            "canonical_player_name",
            "Most recent observed player handle (literal 'nan' preserved as text).",
        ),
        col_doc(
            "player_name_history",
            "player_name_history",
            "Sorted array of all historical handles observed for this player_id.",
        ),
        col_doc(
            "has_source_id",
            "has_source_id",
            "True if backed by a source VLR Player ID; False for ID-less 2021 players with surrogate keys.",
        ),
        col_doc("first_seen_year", "first_seen_year", "First VCT season year this player appeared."),
        col_doc("last_seen_year", "last_seen_year", "Most recent VCT season year this player appeared."),
    )


@dp.materialized_view(
    name=f"{GOLD}.dim_agent",
    comment="Gold agent dimension with verified first_seen_year in VCT matches and release metadata.",
    table_properties={"quality": "gold"},
)
def gold_dim_agent():
    df = spark.read.table(f"{SILVER}.dim_agent")
    return df.select(
        col_doc("agent", "agent", "Lowercase Valorant agent name (PK)."),
        col_doc(
            "first_seen_year",
            "first_seen_year",
            "First VCT season year this agent was actually played in matches_overview.",
        ),
        col_doc(
            "release_date", "release_date", "Approximate agent release date (YYYY-MM-DD); check release_date_verified."
        ),
        col_doc(
            "release_date_verified",
            "release_date_verified",
            "True if release_date was verified against official Riot patch notes.",
        ),
    )


@dp.materialized_view(
    name=f"{GOLD}.dim_map",
    comment="Gold Valorant map dimension (excludes 'All Maps' rollup placeholder).",
    table_properties={"quality": "gold"},
)
def gold_dim_map():
    df = spark.read.table(f"{SILVER}.dim_map")
    return df.select(
        col_doc("map_name", "map_name", "Valorant map name (PK)."),
        col_doc("first_seen_year", "first_seen_year", "First VCT season year this map appeared."),
        col_doc("last_seen_year", "last_seen_year", "Most recent VCT season year this map appeared."),
    )
