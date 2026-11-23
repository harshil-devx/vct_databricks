"""Serverless job task: validate landing volume snapshots and file non-emptiness.

Runs before the vct_medallion pipeline to fail fast if expected CSVs are missing
or truncated in the Unity Catalog landing volume.
"""

from __future__ import annotations

import argparse
import pathlib

YEARLY_FILES: tuple[str, ...] = (
    "agents/agents_pick_rates.csv",
    "agents/maps_stats.csv",
    "agents/teams_picked_agents.csv",
    "ids/players_ids.csv",
    "ids/teams_ids.csv",
    "ids/tournaments_stages_match_types_ids.csv",
    "ids/tournaments_stages_matches_games_ids.csv",
    "matches/draft_phase.csv",
    "matches/eco_rounds.csv",
    "matches/eco_stats.csv",
    "matches/kills.csv",
    "matches/kills_stats.csv",
    "matches/maps_played.csv",
    "matches/maps_scores.csv",
    "matches/overview.csv",
    "matches/rounds_kills.csv",
    "matches/scores.csv",
    "matches/team_mapping.csv",
    "matches/win_loss_methods_count.csv",
    "matches/win_loss_methods_round_number.csv",
    "players_stats/players_stats.csv",
)

ALL_IDS_FILES: tuple[str, ...] = (
    "all_ids/all_matches_games_ids.csv",
    "all_ids/all_players_ids.csv",
    "all_ids/all_teams_ids.csv",
    "all_ids/all_teams_mapping.csv",
    "all_ids/all_tournaments_stages_match_types_ids.csv",
)


def main() -> None:
    parser = argparse.ArgumentParser(description="Validate VCT landing volume snapshots.")
    parser.add_argument(
        "--landing-root",
        required=True,
        help="UC Volume root, e.g. /Volumes/final_project/vct_landing/raw/vct",
    )
    args = parser.parse_args()

    root = pathlib.Path(args.landing_root)
    if not root.exists():
        raise RuntimeError(f"Landing root does not exist: {root}")

    snapshots = sorted([p for p in root.iterdir() if p.is_dir()])
    if not snapshots:
        raise RuntimeError(f"No snapshot folders found under {root}")

    missing: list[str] = []
    empty: list[str] = []
    yearly_checked = 0
    all_ids_checked = 0

    for snap in snapshots:
        year_dirs = sorted([d for d in snap.iterdir() if d.is_dir() and d.name.startswith("vct_")])
        for ydir in year_dirs:
            for rel in YEARLY_FILES:
                f = ydir / rel
                if not f.exists():
                    missing.append(str(f))
                elif f.stat().st_size == 0:
                    empty.append(str(f))
                else:
                    yearly_checked += 1

        all_ids_dir = snap / "all_ids"
        if all_ids_dir.is_dir():
            for rel in ALL_IDS_FILES:
                f = snap / rel
                if not f.exists():
                    missing.append(str(f))
                elif f.stat().st_size == 0:
                    empty.append(str(f))
                else:
                    all_ids_checked += 1

    print(f"Snapshots found ({len(snapshots)}): {[s.name for s in snapshots]}")
    print(f"Validated {yearly_checked} yearly CSVs and {all_ids_checked} all_ids CSVs.")

    if missing or empty:
        raise RuntimeError(f"Landing validation failed! missing={missing[:10]}, empty={empty[:10]}")
    if yearly_checked == 0 or all_ids_checked == 0:
        raise RuntimeError(
            f"Expected both yearly and all_ids files; got yearly={yearly_checked}, all_ids={all_ids_checked}"
        )

    print("Landing validation PASSED.")


if __name__ == "__main__":
    main()
