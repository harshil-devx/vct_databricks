"""Gold star-schema core facts (strictly one grain each, map/match non-rollup only)."""

from __future__ import annotations

from pyspark import pipelines as dp
from pyspark.sql import functions as F

from pipelines.gold.gold_utils import col_doc

SILVER = spark.conf.get("vct.silver_schema")
GOLD = spark.conf.get("vct.gold_schema")


@dp.materialized_view(
    name=f"{GOLD}.fct_game_scores",
    comment="Gold map/game fact (grain: 1 row per game) with tournament tier, stats coverage flags, and gap reason.",
    table_properties={"quality": "gold"},
)
def gold_fct_game_scores():
    games = spark.read.table(f"{SILVER}.fct_map_game").filter(F.col("game_id").isNotNull())
    tourns = spark.read.table(f"{SILVER}.dim_tournament").select(
        F.col("tournament_id").alias("_tid"),
        F.col("tournament_tier").alias("tournament_tier"),
    )
    return games.join(tourns, games["tournament_id"] == tourns["_tid"], how="left").select(
        col_doc("game_id", "game_id", "Unique VLR game/map identifier (PK)."),
        col_doc("match_id", "match_id", "Parent match identifier (FK)."),
        col_doc("tournament_id", "tournament_id", "Tournament identifier (FK to dim_tournament)."),
        col_doc("year", "year", "VCT season year."),
        col_doc(F.coalesce(F.col("tournament_tier"), F.lit("Other")), "tournament_tier", "Analytical tournament tier."),
        col_doc("map", "map_name", "Map played (FK to dim_map)."),
        col_doc("team_a_id", "team_a_id", "Team A identifier (FK to dim_team)."),
        col_doc("team_b_id", "team_b_id", "Team B identifier (FK to dim_team)."),
        col_doc("winner_team_id", "winner_team_id", "Winning team identifier (NULL on draw)."),
        col_doc("team_a_score", "team_a_score", "Total rounds won by Team A."),
        col_doc("team_a_attacker_score", "team_a_attacker_score", "Attack-half rounds won by Team A."),
        col_doc("team_a_defender_score", "team_a_defender_score", "Defense-half rounds won by Team A."),
        col_doc(
            "team_a_overtime_score", "team_a_overtime_score", "Overtime rounds won by Team A (0 if is_overtime=false)."
        ),
        col_doc("team_b_score", "team_b_score", "Total rounds won by Team B."),
        col_doc("team_b_attacker_score", "team_b_attacker_score", "Attack-half rounds won by Team B."),
        col_doc("team_b_defender_score", "team_b_defender_score", "Defense-half rounds won by Team B."),
        col_doc(
            "team_b_overtime_score", "team_b_overtime_score", "Overtime rounds won by Team B (0 if is_overtime=false)."
        ),
        col_doc("is_overtime", "is_overtime", "True if the map went to overtime."),
        col_doc("is_draw", "is_draw", "True if the map ended in a draw."),
        col_doc("is_showmatch", "is_showmatch", "True if the game belongs to a Showmatch."),
        col_doc("duration_seconds", "duration_seconds", "Map duration in seconds (NULL if unrecorded)."),
        col_doc(
            "has_performance_stats",
            "has_performance_stats",
            "True if player multi-kill/clutch performance stats exist for this game.",
        ),
        col_doc("has_economy_stats", "has_economy_stats", "True if round-level economy stats exist for this game."),
        col_doc(
            "documented_gap_reason", "documented_gap_reason", "Provider-documented stats gap reason (NULL if none)."
        ),
    )


@dp.materialized_view(
    name=f"{GOLD}.fct_player_game_stats",
    comment="Gold player-game fact (grain: 1 row per game-player, side='both', non-rollup map rows only). Source for all additive player metrics.",
    table_properties={"quality": "gold"},
)
def gold_fct_player_game_stats():
    stats = spark.read.table(f"{SILVER}.fct_player_map_stats").filter(
        (F.col("side") == "both") & (~F.col("is_map_rollup")) & F.col("game_id").isNotNull()
    )
    tourns = spark.read.table(f"{SILVER}.dim_tournament").select(
        F.col("tournament_id").alias("_tid"),
        F.col("tournament_tier").alias("tournament_tier"),
    )
    return stats.join(tourns, stats["tournament_id"] == tourns["_tid"], how="left").select(
        col_doc("game_id", "game_id", "Game identifier (FK to fct_game_scores)."),
        col_doc("match_id", "match_id", "Match identifier (FK to match_results_base)."),
        col_doc("tournament_id", "tournament_id", "Tournament identifier (FK to dim_tournament)."),
        col_doc("year", "year", "VCT season year."),
        col_doc(F.coalesce(F.col("tournament_tier"), F.lit("Other")), "tournament_tier", "Analytical tournament tier."),
        col_doc("map", "map_name", "Map played (FK to dim_map)."),
        col_doc("player_id", "player_id", "Conformed player identifier (FK to dim_player)."),
        col_doc("team_id", "team_id", "Conformed team identifier (FK to dim_team)."),
        col_doc("agent", "agent", "Agent played (FK to dim_agent)."),
        col_doc("rating", "rating", "VLR 2.0 rating (NULL when unavailable; never zero-filled)."),
        col_doc("acs", "acs", "Average Combat Score (NULL when unavailable)."),
        col_doc("kills", "kills", "Kills on this map (additive)."),
        col_doc("deaths", "deaths", "Deaths on this map (additive)."),
        col_doc("assists", "assists", "Assists on this map (additive)."),
        col_doc("kd_diff", "kd_diff", "Kills minus deaths on this map."),
        col_doc("kast_pct", "kast_pct", "Kill/Assist/Survive/Trade percentage as decimal 0-1 (NULL when unavailable)."),
        col_doc("adr", "adr", "Average Damage per Round (NULL when unavailable)."),
        col_doc("hs_pct", "hs_pct", "Headshot percentage as decimal 0-1 (NULL when unavailable)."),
        col_doc("first_kills", "first_kills", "First kills on this map (additive)."),
        col_doc("first_deaths", "first_deaths", "First deaths on this map (additive)."),
        col_doc("fk_fd_diff", "fk_fd_diff", "First kills minus first deaths."),
        col_doc("is_showmatch", "is_showmatch", "True if this game is a Showmatch."),
        col_doc(
            "documented_gap_reason", "documented_gap_reason", "Provider-documented stats gap reason (NULL if none)."
        ),
    )
