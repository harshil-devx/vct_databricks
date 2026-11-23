"""Pure-Python unit tests for Bronze column naming and table registry."""

import pytest

from pipelines.bronze.bronze_config import BRONZE_TABLES, snake_names, to_snake


@pytest.mark.parametrize(
    ("raw", "expected"),
    [
        ("Tournament", "tournament"),
        ("Kill, Assist, Trade, Survive %", "kill_assist_trade_survive_pct"),
        ("Kills - Deaths (KD)", "kills_deaths_kd"),
        ("Headshot %", "headshot_pct"),
        ("Clutch Success %", "clutch_success_pct"),
        ("  Average Combat Score  ", "average_combat_score"),
        ("First Kills - First Deaths (FKFD)", "first_kills_first_deaths_fkfd"),
        ("2K", "2k"),
        ("   ", "col"),
        ("%%%", "pct_pct_pct"),
    ],
)
def test_to_snake(raw: str, expected: str) -> None:
    assert to_snake(raw) == expected


def test_snake_names_preserves_rescued_data() -> None:
    cols = ["Tournament", "Headshot %", "_rescued_data"]
    assert snake_names(cols) == ["tournament", "headshot_pct", "_rescued_data"]


def test_snake_names_raises_on_collision() -> None:
    with pytest.raises(ValueError, match="collide"):
        snake_names(["Team A", "Team-A"])


def test_snake_names_raises_on_pct_collision() -> None:
    with pytest.raises(ValueError, match="collide"):
        snake_names(["Win %", "Win pct"])


def test_bronze_registry_has_26_unique_tables() -> None:
    assert len(BRONZE_TABLES) == 26
    yearly = [t for t in BRONZE_TABLES if t.is_yearly]
    all_ids = [t for t in BRONZE_TABLES if not t.is_yearly]
    assert len(yearly) == 21
    assert len(all_ids) == 5
    names = [t.table_name for t in BRONZE_TABLES]
    assert len(set(names)) == 26
    for spec in all_ids:
        assert spec.raw_columns is not None
        # Ensure all_ids raw headers convert cleanly without collisions
        snake_names(spec.raw_columns)
