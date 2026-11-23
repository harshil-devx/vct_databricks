"""Pure-Python configuration and column-naming helpers for VCT Bronze tables.

Kept free of PySpark imports so unit tests can validate naming rules, collision
detection, and the 26-table registry locally without a Spark session.
"""

from __future__ import annotations

import re
from collections.abc import Sequence
from dataclasses import dataclass


@dataclass(frozen=True)
class BronzeTableSpec:
    """Declarative specification for one Auto Loader bronze streaming table."""

    table_name: str
    rel_path: str
    is_yearly: bool
    comment: str
    raw_columns: tuple[str, ...] | None = None


def to_snake(name: str) -> str:
    """Convert a raw CSV column header into a Delta-safe snake_case identifier."""
    s = name.strip().lower().replace("%", " pct ")
    s = re.sub(r"[^a-z0-9]+", "_", s).strip("_")
    return s or "col"


def snake_names(columns: Sequence[str]) -> list[str]:
    """Rename CSV columns to snake_case, preserving _rescued_data and failing on collisions."""
    renamed = [c if c == "_rescued_data" else to_snake(c) for c in columns]
    dupes = sorted({n for n in renamed if renamed.count(n) > 1})
    if dupes:
        raise ValueError(f"column names collide after snake_case: {dupes}")
    return renamed


BRONZE_TABLES: tuple[BronzeTableSpec, ...] = (
    # --- Yearly files (21) ---
    BronzeTableSpec(
        "agents_pick_rates",
        "agents/agents_pick_rates.csv",
        True,
        "Raw agent pick rates by tournament/stage/map from agents/agents_pick_rates.csv.",
    ),
    BronzeTableSpec(
        "agents_maps_stats",
        "agents/maps_stats.csv",
        True,
        "Raw map attack/defense side stats from agents/maps_stats.csv.",
    ),
    BronzeTableSpec(
        "agents_teams_picked",
        "agents/teams_picked_agents.csv",
        True,
        "Raw team agent picks from agents/teams_picked_agents.csv.",
    ),
    BronzeTableSpec(
        "ids_players",
        "ids/players_ids.csv",
        True,
        "Raw yearly player name to Player ID mapping from ids/players_ids.csv.",
    ),
    BronzeTableSpec(
        "ids_teams",
        "ids/teams_ids.csv",
        True,
        "Raw yearly team name to Team ID mapping from ids/teams_ids.csv.",
    ),
    BronzeTableSpec(
        "ids_tournament_stage_match_types",
        "ids/tournaments_stages_match_types_ids.csv",
        True,
        "Raw yearly tournament/stage/match-type IDs from ids/tournaments_stages_match_types_ids.csv.",
    ),
    BronzeTableSpec(
        "ids_tournament_stage_matches_games",
        "ids/tournaments_stages_matches_games_ids.csv",
        True,
        "Raw yearly match and game IDs from ids/tournaments_stages_matches_games_ids.csv.",
    ),
    BronzeTableSpec(
        "matches_draft_phase",
        "matches/draft_phase.csv",
        True,
        "Raw map veto (ban/pick) actions from matches/draft_phase.csv.",
    ),
    BronzeTableSpec(
        "matches_eco_rounds",
        "matches/eco_rounds.csv",
        True,
        "Raw round-level economy and loadout values from matches/eco_rounds.csv.",
    ),
    BronzeTableSpec(
        "matches_eco_stats",
        "matches/eco_stats.csv",
        True,
        "Raw long-format economy summary stats from matches/eco_stats.csv.",
    ),
    BronzeTableSpec(
        "matches_kills",
        "matches/kills.csv",
        True,
        "Raw player-pair head-to-head kills (All/First/Op) from matches/kills.csv.",
    ),
    BronzeTableSpec(
        "matches_kills_stats",
        "matches/kills_stats.csv",
        True,
        "Raw player multi-kill and clutch stats from matches/kills_stats.csv.",
    ),
    BronzeTableSpec(
        "matches_maps_played",
        "matches/maps_played.csv",
        True,
        "Raw maps played per match from matches/maps_played.csv.",
    ),
    BronzeTableSpec(
        "matches_maps_scores",
        "matches/maps_scores.csv",
        True,
        "Raw map-level scores (attack/defense/overtime) from matches/maps_scores.csv.",
    ),
    BronzeTableSpec(
        "matches_overview",
        "matches/overview.csv",
        True,
        "Raw player-per-map stats from matches/overview.csv. All columns are strings.",
    ),
    BronzeTableSpec(
        "matches_rounds_kills",
        "matches/rounds_kills.csv",
        True,
        "Raw round-level kill events from matches/rounds_kills.csv.",
    ),
    BronzeTableSpec(
        "matches_scores",
        "matches/scores.csv",
        True,
        "Raw match-level scores and results from matches/scores.csv.",
    ),
    BronzeTableSpec(
        "matches_team_mapping",
        "matches/team_mapping.csv",
        True,
        "Raw team abbreviation to full name mapping from matches/team_mapping.csv.",
    ),
    BronzeTableSpec(
        "matches_win_loss_methods_count",
        "matches/win_loss_methods_count.csv",
        True,
        "Raw map-level round win/loss method counts from matches/win_loss_methods_count.csv.",
    ),
    BronzeTableSpec(
        "matches_win_loss_methods_round_number",
        "matches/win_loss_methods_round_number.csv",
        True,
        "Raw round-by-round win/loss outcomes from matches/win_loss_methods_round_number.csv.",
    ),
    BronzeTableSpec(
        "players_stats",
        "players_stats/players_stats.csv",
        True,
        "Raw player tournament/stage summary stats from players_stats/players_stats.csv.",
    ),
    # --- Cross-year all_ids files (5) ---
    # Explicit all-STRING raw_columns let Auto Loader initialize cleanly even before
    # s07_2026_full uploads all_ids/, while still rescuing any unexpected extra columns.
    BronzeTableSpec(
        "all_ids_matches_games",
        "all_ids/all_matches_games_ids.csv",
        False,
        "Raw cross-year match and game IDs from all_ids/all_matches_games_ids.csv.",
        raw_columns=(
            "Tournament",
            "Tournament ID",
            "Stage",
            "Stage ID",
            "Match Type",
            "Match Type ID",
            "Match Name",
            "Match ID",
            "Map",
            "Game ID",
            "Year",
        ),
    ),
    BronzeTableSpec(
        "all_ids_players",
        "all_ids/all_players_ids.csv",
        False,
        "Raw cross-year player IDs from all_ids/all_players_ids.csv.",
        raw_columns=("Player", "Player ID"),
    ),
    BronzeTableSpec(
        "all_ids_teams",
        "all_ids/all_teams_ids.csv",
        False,
        "Raw cross-year team IDs from all_ids/all_teams_ids.csv.",
        raw_columns=("Team", "Team ID"),
    ),
    BronzeTableSpec(
        "all_ids_teams_mapping",
        "all_ids/all_teams_mapping.csv",
        False,
        "Raw cross-year team abbreviation mapping from all_ids/all_teams_mapping.csv.",
        raw_columns=("Abbreviated", "Full Name"),
    ),
    BronzeTableSpec(
        "all_ids_tournament_stage_match_types",
        "all_ids/all_tournaments_stages_match_types_ids.csv",
        False,
        "Raw cross-year tournament/stage/match-type IDs from all_ids/all_tournaments_stages_match_types_ids.csv.",
        raw_columns=(
            "Tournament",
            "Tournament ID",
            "Stage",
            "Stage ID",
            "Match Type",
            "Match Type ID",
            "Year",
        ),
    ),
)
