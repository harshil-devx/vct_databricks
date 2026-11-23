"""Gold analyst-ready marts (8 marts) with coverage columns, tiers, gap flags, and comments."""

from __future__ import annotations

from pyspark import pipelines as dp
from pyspark.sql import Window
from pyspark.sql import functions as F

from pipelines.gold.gold_utils import col_doc

SILVER = spark.conf.get("vct.silver_schema")
GOLD = spark.conf.get("vct.gold_schema")


@dp.materialized_view(
    name=f"{GOLD}.match_results_base",
    comment="Analyst-ready match results mart (grain: 1 row per match) with tiers, canonical team names, draw/showmatch flags, and gap reason.",
    table_properties={"quality": "gold"},
)
def gold_match_results_base():
    matches = spark.read.table(f"{SILVER}.fct_match")
    tourns = spark.read.table(f"{GOLD}.dim_tournament").select(
        F.col("tournament_id").alias("_tid"),
        F.col("tournament_tier").alias("tournament_tier"),
    )
    teams_a = spark.read.table(f"{GOLD}.dim_team").select(
        F.col("team_id").alias("_ta_id"),
        F.col("canonical_team_name").alias("team_a_canonical_name"),
    )
    teams_b = spark.read.table(f"{GOLD}.dim_team").select(
        F.col("team_id").alias("_tb_id"),
        F.col("canonical_team_name").alias("team_b_canonical_name"),
    )
    teams_w = spark.read.table(f"{GOLD}.dim_team").select(
        F.col("team_id").alias("_tw_id"),
        F.col("canonical_team_name").alias("winner_team_name"),
    )
    map_counts = spark.read.table(f"{GOLD}.fct_game_scores").groupBy("match_id").agg(F.count("*").alias("maps_played"))

    return (
        matches.join(tourns, matches["tournament_id"] == tourns["_tid"], how="left")
        .join(teams_a, matches["team_a_id"] == teams_a["_ta_id"], how="left")
        .join(teams_b, matches["team_b_id"] == teams_b["_tb_id"], how="left")
        .join(teams_w, matches["winner_team_id"] == teams_w["_tw_id"], how="left")
        .join(map_counts, on="match_id", how="left")
        .select(
            col_doc("match_id", "match_id", "Unique VLR match identifier."),
            col_doc("year", "year", "VCT season year."),
            col_doc("tournament_id", "tournament_id", "Tournament identifier (FK to dim_tournament)."),
            col_doc("tournament", "tournament_name", "Tournament name."),
            col_doc(
                F.coalesce(F.col("tournament_tier"), F.lit("Other")), "tournament_tier", "Analytical tournament tier."
            ),
            col_doc("stage", "stage", "Tournament stage (e.g. Playoffs, Group Stage, Showmatch)."),
            col_doc("match_type", "match_type", "Match bracket/round type."),
            col_doc("match_name", "match_name", "Source match display name."),
            col_doc("team_a_id", "team_a_id", "Team A identifier."),
            col_doc(
                F.coalesce(F.col("team_a_canonical_name"), F.col("team_a_name")),
                "team_a_name",
                "Team A canonical name.",
            ),
            col_doc("team_b_id", "team_b_id", "Team B identifier."),
            col_doc(
                F.coalesce(F.col("team_b_canonical_name"), F.col("team_b_name")),
                "team_b_name",
                "Team B canonical name.",
            ),
            col_doc("team_a_score", "team_a_score", "Maps won by Team A."),
            col_doc("team_b_score", "team_b_score", "Maps won by Team B."),
            col_doc("maps_played", "maps_played", "Count of resolved map games played in this match."),
            col_doc("winner_team_id", "winner_team_id", "Winning team_id (NULL on draws)."),
            col_doc("winner_team_name", "winner_team_name", "Winning team canonical name (NULL on draws)."),
            col_doc("is_draw", "is_draw", "True if the match ended in a draw."),
            col_doc("is_showmatch", "is_showmatch", "True if this match is a Showmatch."),
            col_doc(
                "documented_gap_reason", "documented_gap_reason", "Provider-documented stats gap reason (NULL if none)."
            ),
        )
    )


@dp.materialized_view(
    name=f"{GOLD}.player_career_summary",
    comment="Career player performance mart (grain: 1 row per player_id). Built strictly from non-rollup map games; averages ignore NULLs with coverage ratio.",
    table_properties={"quality": "gold"},
)
def gold_player_career_summary():
    pg = spark.read.table(f"{GOLD}.fct_player_game_stats").filter(~F.col("is_showmatch"))
    players = spark.read.table(f"{GOLD}.dim_player")

    top_agent_w = Window.partitionBy("player_id").orderBy(F.col("_agent_games").desc(), F.col("agent").asc())
    top_agent = (
        pg.filter(F.col("agent").isNotNull())
        .groupBy("player_id", "agent")
        .agg(F.count("*").alias("_agent_games"))
        .withColumn("_rn", F.row_number().over(top_agent_w))
        .filter(F.col("_rn") == 1)
        .select("player_id", F.col("agent").alias("most_played_agent"))
    )

    agg = pg.groupBy("player_id").agg(
        F.count("*").alias("games_played"),
        F.countDistinct("match_id").alias("matches_played"),
        F.count("rating").alias("games_with_rating"),
        F.sum("kills").alias("total_kills"),
        F.sum("deaths").alias("total_deaths"),
        F.sum("assists").alias("total_assists"),
        F.sum("first_kills").alias("total_first_kills"),
        F.sum("first_deaths").alias("total_first_deaths"),
        F.round(F.avg("rating"), 3).alias("avg_rating"),
        F.round(F.avg("acs"), 1).alias("avg_acs"),
        F.round(F.avg("adr"), 1).alias("avg_adr"),
        F.round(F.avg("hs_pct"), 4).alias("avg_hs_pct"),
        F.round(F.avg("kast_pct"), 4).alias("avg_kast_pct"),
        F.sum(F.when(F.col("documented_gap_reason").isNotNull(), 1).otherwise(0)).alias("games_with_documented_gap"),
    )

    return (
        agg.join(players, on="player_id", how="left")
        .join(top_agent, on="player_id", how="left")
        .select(
            col_doc("player_id", "player_id", "Conformed player identifier (PK)."),
            col_doc("canonical_player_name", "canonical_player_name", "Most recent player handle."),
            col_doc("has_source_id", "has_source_id", "True if backed by a source VLR Player ID."),
            col_doc("first_seen_year", "first_seen_year", "First VCT season year."),
            col_doc("last_seen_year", "last_seen_year", "Most recent VCT season year."),
            col_doc(
                "most_played_agent", "most_played_agent", "Most frequently played agent across non-showmatch maps."
            ),
            col_doc("games_played", "games_played", "Total non-showmatch map games played."),
            col_doc("matches_played", "matches_played", "Distinct non-showmatch matches played."),
            col_doc("games_with_rating", "games_with_rating", "Games with non-NULL VLR rating."),
            col_doc(
                F.round(F.col("games_with_rating") / F.col("games_played"), 4),
                "rating_coverage_pct",
                "Share of games with non-NULL rating (games_with_rating / games_played).",
            ),
            col_doc("total_kills", "total_kills", "Career kills (sum of map-level non-rollup games)."),
            col_doc("total_deaths", "total_deaths", "Career deaths."),
            col_doc("total_assists", "total_assists", "Career assists."),
            col_doc(
                F.round(F.col("total_kills") / F.nullif(F.col("total_deaths"), F.lit(0)), 3),
                "career_kd_ratio",
                "Career kills / deaths ratio.",
            ),
            col_doc("total_first_kills", "total_first_kills", "Career first kills."),
            col_doc("total_first_deaths", "total_first_deaths", "Career first deaths."),
            col_doc("avg_rating", "avg_rating", "Average VLR rating across games with stats (NULLs ignored)."),
            col_doc("avg_acs", "avg_acs", "Average Combat Score (NULLs ignored)."),
            col_doc("avg_adr", "avg_adr", "Average Damage per Round (NULLs ignored)."),
            col_doc("avg_hs_pct", "avg_hs_pct", "Average Headshot % as decimal 0-1 (NULLs ignored)."),
            col_doc("avg_kast_pct", "avg_kast_pct", "Average KAST % as decimal 0-1 (NULLs ignored)."),
            col_doc(
                "games_with_documented_gap",
                "games_with_documented_gap",
                "Count of games in China-hosted or other documented-gap tournaments.",
            ),
        )
    )


@dp.materialized_view(
    name=f"{GOLD}.player_season_form",
    comment="Player season form mart (grain: 1 row per player_id, year, tournament_tier). Excludes showmatches; includes stats coverage ratio.",
    table_properties={"quality": "gold"},
)
def gold_player_season_form():
    pg = spark.read.table(f"{GOLD}.fct_player_game_stats").filter(~F.col("is_showmatch"))
    players = spark.read.table(f"{GOLD}.dim_player").select("player_id", "canonical_player_name")

    agg = pg.groupBy("player_id", "year", "tournament_tier").agg(
        F.count("*").alias("games_played"),
        F.count("rating").alias("games_with_rating"),
        F.sum("kills").alias("total_kills"),
        F.sum("deaths").alias("total_deaths"),
        F.sum("assists").alias("total_assists"),
        F.round(F.avg("rating"), 3).alias("avg_rating"),
        F.round(F.avg("acs"), 1).alias("avg_acs"),
        F.round(F.avg("adr"), 1).alias("avg_adr"),
        F.round(F.avg("hs_pct"), 4).alias("avg_hs_pct"),
        F.round(F.avg("kast_pct"), 4).alias("avg_kast_pct"),
        F.sum(F.when(F.col("documented_gap_reason").isNotNull(), 1).otherwise(0)).alias("games_with_documented_gap"),
    )

    return agg.join(players, on="player_id", how="left").select(
        col_doc("player_id", "player_id", "Conformed player identifier."),
        col_doc("canonical_player_name", "canonical_player_name", "Player canonical handle."),
        col_doc("year", "year", "VCT season year."),
        col_doc("tournament_tier", "tournament_tier", "Analytical tournament tier."),
        col_doc("games_played", "games_played", "Map games played in this year and tier."),
        col_doc("games_with_rating", "games_with_rating", "Games with non-NULL VLR rating."),
        col_doc(
            F.round(F.col("games_with_rating") / F.col("games_played"), 4),
            "rating_coverage_pct",
            "Share of games with non-NULL rating.",
        ),
        col_doc("total_kills", "total_kills", "Season kills in this tier."),
        col_doc("total_deaths", "total_deaths", "Season deaths in this tier."),
        col_doc("total_assists", "total_assists", "Season assists in this tier."),
        col_doc(
            F.round(F.col("total_kills") / F.nullif(F.col("total_deaths"), F.lit(0)), 3),
            "kd_ratio",
            "Season kills / deaths ratio.",
        ),
        col_doc("avg_rating", "avg_rating", "Average VLR rating (NULLs ignored)."),
        col_doc("avg_acs", "avg_acs", "Average Combat Score (NULLs ignored)."),
        col_doc("avg_adr", "avg_adr", "Average Damage per Round (NULLs ignored)."),
        col_doc("avg_hs_pct", "avg_hs_pct", "Average Headshot % as decimal 0-1."),
        col_doc("avg_kast_pct", "avg_kast_pct", "Average KAST % as decimal 0-1."),
        col_doc("games_with_documented_gap", "games_with_documented_gap", "Games in documented-gap tournaments."),
    )


@dp.materialized_view(
    name=f"{GOLD}.team_results_timeline",
    comment="Team tournament results timeline mart (grain: 1 row per team_id, year, tournament_id). Excludes TBD and showmatches.",
    table_properties={"quality": "gold"},
)
def gold_team_results_timeline():
    m = spark.read.table(f"{GOLD}.match_results_base").filter(~F.col("is_showmatch"))
    side_a = m.select(
        F.col("team_a_id").alias("team_id"),
        "year",
        "tournament_id",
        "tournament_name",
        "tournament_tier",
        "documented_gap_reason",
        "is_draw",
        (F.col("winner_team_id") == F.col("team_a_id")).alias("is_win"),
        F.col("team_a_score").alias("maps_won"),
        F.col("team_b_score").alias("maps_lost"),
    )
    side_b = m.select(
        F.col("team_b_id").alias("team_id"),
        "year",
        "tournament_id",
        "tournament_name",
        "tournament_tier",
        "documented_gap_reason",
        "is_draw",
        (F.col("winner_team_id") == F.col("team_b_id")).alias("is_win"),
        F.col("team_b_score").alias("maps_won"),
        F.col("team_a_score").alias("maps_lost"),
    )
    both = side_a.unionByName(side_b).filter(F.col("team_id").isNotNull() & (F.col("team_id") != "TBD"))
    teams = spark.read.table(f"{GOLD}.dim_team").select("team_id", "canonical_team_name", "alias_status")

    agg = both.groupBy(
        "team_id", "year", "tournament_id", "tournament_name", "tournament_tier", "documented_gap_reason"
    ).agg(
        F.count("*").alias("matches_played"),
        F.sum(F.when(F.col("is_win"), 1).otherwise(0)).alias("matches_won"),
        F.sum(F.when(~F.col("is_win") & ~F.col("is_draw"), 1).otherwise(0)).alias("matches_lost"),
        F.sum(F.when(F.col("is_draw"), 1).otherwise(0)).alias("matches_drawn"),
        F.sum("maps_won").alias("maps_won"),
        F.sum("maps_lost").alias("maps_lost"),
    )

    return agg.join(teams, on="team_id", how="left").select(
        col_doc("team_id", "team_id", "Conformed team identifier."),
        col_doc("canonical_team_name", "canonical_team_name", "Team canonical name."),
        col_doc("alias_status", "alias_status", "Team identity status (verified/unverified/unresolved)."),
        col_doc("year", "year", "VCT season year."),
        col_doc("tournament_id", "tournament_id", "Tournament identifier."),
        col_doc("tournament_name", "tournament_name", "Tournament name."),
        col_doc("tournament_tier", "tournament_tier", "Analytical tournament tier."),
        col_doc("matches_played", "matches_played", "Non-showmatch matches played in this tournament."),
        col_doc("matches_won", "matches_won", "Matches won."),
        col_doc("matches_lost", "matches_lost", "Matches lost."),
        col_doc("matches_drawn", "matches_drawn", "Matches drawn."),
        col_doc(
            F.round(F.col("matches_won") / F.nullif(F.col("matches_played"), F.lit(0)), 4),
            "match_win_rate",
            "Matches won / matches played.",
        ),
        col_doc("maps_won", "maps_won", "Total maps won."),
        col_doc("maps_lost", "maps_lost", "Total maps lost."),
        col_doc(
            "documented_gap_reason", "documented_gap_reason", "Provider-documented stats gap reason (NULL if none)."
        ),
    )


@dp.materialized_view(
    name=f"{GOLD}.team_map_pool_winrate",
    comment="Team map-pool win rate mart (grain: 1 row per team_id, map_name, year, tournament_tier). Excludes TBD and showmatches.",
    table_properties={"quality": "gold"},
)
def gold_team_map_pool_winrate():
    g = spark.read.table(f"{GOLD}.fct_game_scores").filter(~F.col("is_showmatch"))
    side_a = g.select(
        F.col("team_a_id").alias("team_id"),
        "map_name",
        "year",
        "tournament_tier",
        "documented_gap_reason",
        "is_draw",
        (F.col("winner_team_id") == F.col("team_a_id")).alias("is_win"),
        F.col("team_a_score").alias("rounds_for"),
        F.col("team_b_score").alias("rounds_against"),
        F.col("team_a_attacker_score").alias("atk_rounds_won"),
        F.col("team_a_defender_score").alias("def_rounds_won"),
    )
    side_b = g.select(
        F.col("team_b_id").alias("team_id"),
        "map_name",
        "year",
        "tournament_tier",
        "documented_gap_reason",
        "is_draw",
        (F.col("winner_team_id") == F.col("team_b_id")).alias("is_win"),
        F.col("team_b_score").alias("rounds_for"),
        F.col("team_a_score").alias("rounds_against"),
        F.col("team_b_attacker_score").alias("atk_rounds_won"),
        F.col("team_b_defender_score").alias("def_rounds_won"),
    )
    both = side_a.unionByName(side_b).filter(F.col("team_id").isNotNull() & (F.col("team_id") != "TBD"))
    teams = spark.read.table(f"{GOLD}.dim_team").select("team_id", "canonical_team_name")

    agg = both.groupBy("team_id", "map_name", "year", "tournament_tier").agg(
        F.count("*").alias("games_played"),
        F.sum(F.when(F.col("is_win"), 1).otherwise(0)).alias("games_won"),
        F.sum(F.when(~F.col("is_win") & ~F.col("is_draw"), 1).otherwise(0)).alias("games_lost"),
        F.sum(F.when(F.col("is_draw"), 1).otherwise(0)).alias("games_drawn"),
        F.round(F.avg(F.col("rounds_for") - F.col("rounds_against")), 2).alias("avg_round_diff"),
        F.round(F.avg("atk_rounds_won"), 2).alias("avg_atk_rounds_won"),
        F.round(F.avg("def_rounds_won"), 2).alias("avg_def_rounds_won"),
        F.sum(F.when(F.col("documented_gap_reason").isNotNull(), 1).otherwise(0)).alias("games_with_documented_gap"),
    )

    return agg.join(teams, on="team_id", how="left").select(
        col_doc("team_id", "team_id", "Conformed team identifier."),
        col_doc("canonical_team_name", "canonical_team_name", "Team canonical name."),
        col_doc("map_name", "map_name", "Map name."),
        col_doc("year", "year", "VCT season year."),
        col_doc("tournament_tier", "tournament_tier", "Analytical tournament tier."),
        col_doc("games_played", "games_played", "Non-showmatch games played on this map."),
        col_doc("games_won", "games_won", "Games won on this map."),
        col_doc("games_lost", "games_lost", "Games lost on this map."),
        col_doc("games_drawn", "games_drawn", "Games drawn on this map."),
        col_doc(
            F.round(F.col("games_won") / F.nullif(F.col("games_played"), F.lit(0)), 4),
            "map_win_rate",
            "Games won / games played.",
        ),
        col_doc("avg_round_diff", "avg_round_diff", "Average round differential per game."),
        col_doc("avg_atk_rounds_won", "avg_atk_rounds_won", "Average attack-half rounds won per game."),
        col_doc("avg_def_rounds_won", "avg_def_rounds_won", "Average defense-half rounds won per game."),
        col_doc("games_with_documented_gap", "games_with_documented_gap", "Games in documented-gap tournaments."),
    )


@dp.materialized_view(
    name=f"{GOLD}.agent_meta_by_tournament",
    comment="Agent meta pick-rate mart (grain: 1 row per tournament_id, map_name, agent). Excludes pre-release tournaments via dim_agent.first_seen_year.",
    table_properties={"quality": "gold"},
)
def gold_agent_meta_by_tournament():
    # Compute true pick rate directly from non-rollup map games (2 teams per game -> max 2 picks per game).
    pg = spark.read.table(f"{GOLD}.fct_player_game_stats").filter(~F.col("is_showmatch") & F.col("agent").isNotNull())
    agents = spark.read.table(f"{GOLD}.dim_agent").select("agent", "first_seen_year")
    tourns = spark.read.table(f"{GOLD}.dim_tournament").select(
        "tournament_id", "tournament_name", "year", "tournament_tier", "documented_gap_reason"
    )

    # Team-map slots per (tournament_id, map_name) = 2 * distinct game_id
    map_totals = pg.groupBy("tournament_id", "map_name").agg(
        F.countDistinct("game_id").alias("games_played"),
        (F.countDistinct("game_id") * F.lit(2)).alias("team_map_slots"),
    )
    picks = pg.groupBy("tournament_id", "map_name", "agent").agg(F.count("*").alias("times_picked"))

    return (
        picks.join(map_totals, on=["tournament_id", "map_name"], how="inner")
        .join(tourns, on="tournament_id", how="inner")
        .join(agents, on="agent", how="inner")
        # Exclude tournaments prior to the agent's verified first_seen_year
        .filter(F.col("year") >= F.col("first_seen_year"))
        .select(
            col_doc("tournament_id", "tournament_id", "Tournament identifier."),
            col_doc("tournament_name", "tournament_name", "Tournament name."),
            col_doc("year", "year", "VCT season year."),
            col_doc("tournament_tier", "tournament_tier", "Analytical tournament tier."),
            col_doc("map_name", "map_name", "Map name."),
            col_doc("agent", "agent", "Agent name."),
            col_doc("first_seen_year", "agent_first_seen_year", "First VCT season year this agent appeared."),
            col_doc("games_played", "games_played", "Distinct games played on this map in this tournament."),
            col_doc("times_picked", "times_picked", "Number of team-map compositions featuring this agent."),
            col_doc(
                F.round(F.col("times_picked") / F.nullif(F.col("team_map_slots"), F.lit(0)), 4),
                "pick_rate",
                "Agent pick rate = times_picked / (2 * games_played).",
            ),
            col_doc(
                "documented_gap_reason", "documented_gap_reason", "Provider-documented stats gap reason (NULL if none)."
            ),
        )
    )


@dp.materialized_view(
    name=f"{GOLD}.map_side_balance",
    comment="Map attacker vs defender side balance mart (grain: 1 row per map_name, year, tournament_tier). Built from non-rollup fct_game_scores.",
    table_properties={"quality": "gold"},
)
def gold_map_side_balance():
    g = spark.read.table(f"{GOLD}.fct_game_scores").filter(~F.col("is_showmatch"))
    agg = g.groupBy("map_name", "year", "tournament_tier").agg(
        F.count("*").alias("games_played"),
        F.sum(
            F.coalesce(F.col("team_a_attacker_score"), F.lit(0)) + F.coalesce(F.col("team_b_attacker_score"), F.lit(0))
        ).alias("total_attacker_rounds_won"),
        F.sum(
            F.coalesce(F.col("team_a_defender_score"), F.lit(0)) + F.coalesce(F.col("team_b_defender_score"), F.lit(0))
        ).alias("total_defender_rounds_won"),
        F.sum(F.when(F.col("is_overtime"), 1).otherwise(0)).alias("overtime_games"),
        F.count("duration_seconds").alias("games_with_duration"),
        F.round(F.avg("duration_seconds"), 0).cast("int").alias("avg_duration_seconds"),
        F.sum(F.when(F.col("documented_gap_reason").isNotNull(), 1).otherwise(0)).alias("games_with_documented_gap"),
    )
    total_reg_rounds = F.col("total_attacker_rounds_won") + F.col("total_defender_rounds_won")
    return agg.select(
        col_doc("map_name", "map_name", "Map name."),
        col_doc("year", "year", "VCT season year."),
        col_doc("tournament_tier", "tournament_tier", "Analytical tournament tier."),
        col_doc("games_played", "games_played", "Non-showmatch games played on this map."),
        col_doc("total_attacker_rounds_won", "total_attacker_rounds_won", "Total regulation attack rounds won."),
        col_doc("total_defender_rounds_won", "total_defender_rounds_won", "Total regulation defense rounds won."),
        col_doc(
            F.round(F.col("total_attacker_rounds_won") / F.nullif(total_reg_rounds, F.lit(0)), 4),
            "attacker_win_rate",
            "Share of regulation rounds won by attackers.",
        ),
        col_doc(
            F.round(F.col("total_defender_rounds_won") / F.nullif(total_reg_rounds, F.lit(0)), 4),
            "defender_win_rate",
            "Share of regulation rounds won by defenders.",
        ),
        col_doc(
            F.round(F.col("overtime_games") / F.nullif(F.col("games_played"), F.lit(0)), 4),
            "overtime_rate",
            "Share of games reaching overtime.",
        ),
        col_doc("avg_duration_seconds", "avg_duration_seconds", "Average map duration in seconds (NULLs ignored)."),
        col_doc(
            F.round(F.col("games_with_duration") / F.nullif(F.col("games_played"), F.lit(0)), 4),
            "duration_coverage_pct",
            "Share of games with recorded duration.",
        ),
        col_doc("games_with_documented_gap", "games_with_documented_gap", "Games in documented-gap tournaments."),
    )


@dp.materialized_view(
    name=f"{GOLD}.economy_efficiency_by_team",
    comment="Team economy conversion efficiency mart (grain: 1 row per team_id, year, tournament_tier) with economy coverage ratio.",
    table_properties={"quality": "gold"},
)
def gold_economy_efficiency_by_team():
    eco = spark.read.table(f"{SILVER}.fct_economy_summary").filter(F.col("game_id").isNotNull())
    games = spark.read.table(f"{GOLD}.fct_game_scores").filter(~F.col("is_showmatch"))

    # Total games per (team_id, year, tournament_tier) for accurate economy_coverage_pct
    team_games = (
        games.select(F.col("team_a_id").alias("team_id"), "game_id", "year", "tournament_tier", "documented_gap_reason")
        .unionByName(
            games.select(
                F.col("team_b_id").alias("team_id"), "game_id", "year", "tournament_tier", "documented_gap_reason"
            )
        )
        .filter(F.col("team_id").isNotNull() & (F.col("team_id") != "TBD"))
    )

    joined = team_games.join(
        eco.select(
            "game_id",
            "team_id",
            "pistol_won",
            "eco_initiated",
            "eco_won",
            "semi_eco_initiated",
            "semi_eco_won",
            "semi_buy_initiated",
            "semi_buy_won",
            "full_buy_initiated",
            "full_buy_won",
        ),
        on=["game_id", "team_id"],
        how="left",
    )

    teams = spark.read.table(f"{GOLD}.dim_team").select("team_id", "canonical_team_name")

    agg = joined.groupBy("team_id", "year", "tournament_tier").agg(
        F.count("*").alias("games_played"),
        F.count("pistol_won").alias("games_with_economy_stats"),
        F.sum("pistol_won").alias("pistol_rounds_won"),
        F.sum("eco_initiated").alias("eco_initiated"),
        F.sum("eco_won").alias("eco_won"),
        F.sum("semi_eco_initiated").alias("semi_eco_initiated"),
        F.sum("semi_eco_won").alias("semi_eco_won"),
        F.sum("semi_buy_initiated").alias("semi_buy_initiated"),
        F.sum("semi_buy_won").alias("semi_buy_won"),
        F.sum("full_buy_initiated").alias("full_buy_initiated"),
        F.sum("full_buy_won").alias("full_buy_won"),
        F.sum(F.when(F.col("documented_gap_reason").isNotNull(), 1).otherwise(0)).alias("games_with_documented_gap"),
    )

    return agg.join(teams, on="team_id", how="left").select(
        col_doc("team_id", "team_id", "Conformed team identifier."),
        col_doc("canonical_team_name", "canonical_team_name", "Team canonical name."),
        col_doc("year", "year", "VCT season year."),
        col_doc("tournament_tier", "tournament_tier", "Analytical tournament tier."),
        col_doc("games_played", "games_played", "Total non-showmatch games played."),
        col_doc("games_with_economy_stats", "games_with_economy_stats", "Games with economy summary stats."),
        col_doc(
            F.round(F.col("games_with_economy_stats") / F.nullif(F.col("games_played"), F.lit(0)), 4),
            "economy_coverage_pct",
            "Share of games with economy summary stats.",
        ),
        col_doc("pistol_rounds_won", "pistol_rounds_won", "Total pistol rounds won."),
        col_doc(
            F.round(F.col("pistol_rounds_won") / F.nullif(F.col("games_with_economy_stats") * F.lit(2), F.lit(0)), 4),
            "pistol_win_rate",
            "Pistol rounds won / (2 * games_with_economy_stats).",
        ),
        col_doc(
            F.round(F.col("eco_won") / F.nullif(F.col("eco_initiated"), F.lit(0)), 4),
            "eco_win_rate",
            "Eco round conversion rate (eco_won / eco_initiated).",
        ),
        col_doc(
            F.round(F.col("semi_eco_won") / F.nullif(F.col("semi_eco_initiated"), F.lit(0)), 4),
            "semi_eco_win_rate",
            "Semi-eco ($) round conversion rate.",
        ),
        col_doc(
            F.round(F.col("semi_buy_won") / F.nullif(F.col("semi_buy_initiated"), F.lit(0)), 4),
            "semi_buy_win_rate",
            "Semi-buy ($$) round conversion rate.",
        ),
        col_doc(
            F.round(F.col("full_buy_won") / F.nullif(F.col("full_buy_initiated"), F.lit(0)), 4),
            "full_buy_win_rate",
            "Full-buy ($$$) round conversion rate.",
        ),
        col_doc("games_with_documented_gap", "games_with_documented_gap", "Games in documented-gap tournaments."),
    )
