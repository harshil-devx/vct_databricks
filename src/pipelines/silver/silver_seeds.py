"""Silver seed tables loaded from version-controlled CSVs in seeds/."""

from __future__ import annotations

import csv
import pathlib

from pyspark import pipelines as dp
from pyspark.sql.types import BooleanType, IntegerType, StringType, StructField, StructType

SILVER = spark.conf.get("vct.silver_schema")
SEEDS_DIR = pathlib.Path(spark.conf.get("vct.seeds_dir"))


def _read_seed_rows(filename: str) -> list[dict[str, str]]:
    path = SEEDS_DIR / filename
    with path.open("r", encoding="utf-8", newline="") as f:
        return list(csv.DictReader(f))


@dp.materialized_view(
    name=f"{SILVER}.seed_tournament_alias",
    comment="Manual tournament name normalization aliases (2025 eco_rounds legacy prefixes + Masters Bangkok).",
    table_properties={"quality": "silver", "layer": "seed"},
)
def seed_tournament_alias():
    rows = [
        (r["source_tournament"], r["canonical_tournament"], int(r["year"]), r["reason"])
        for r in _read_seed_rows("tournament_alias.csv")
    ]
    schema = StructType(
        [
            StructField("source_tournament", StringType(), False),
            StructField("canonical_tournament", StringType(), False),
            StructField("year", IntegerType(), False),
            StructField("reason", StringType(), True),
        ]
    )
    return spark.createDataFrame(rows, schema=schema)


@dp.materialized_view(
    name=f"{SILVER}.seed_team_alias",
    comment="Manual team alias seed. Unverified or unresolved mappings are explicitly flagged.",
    table_properties={"quality": "silver", "layer": "seed"},
)
def seed_team_alias():
    rows = [
        (
            r["source_team"],
            r["canonical_team_id"] or None,
            r["canonical_team_name"],
            r["status"],
            r["notes"],
        )
        for r in _read_seed_rows("team_alias.csv")
    ]
    schema = StructType(
        [
            StructField("source_team", StringType(), False),
            StructField("canonical_team_id", StringType(), True),
            StructField("canonical_team_name", StringType(), False),
            StructField("status", StringType(), False),
            StructField("notes", StringType(), True),
        ]
    )
    return spark.createDataFrame(rows, schema=schema)


@dp.materialized_view(
    name=f"{SILVER}.seed_agent_release",
    comment="Agent release metadata with first_seen_year verified from bronze matches_overview.",
    table_properties={"quality": "silver", "layer": "seed"},
)
def seed_agent_release():
    rows = [
        (
            r["agent"],
            int(r["first_seen_year"]),
            r["release_date"] or None,
            r["release_date_verified"].lower() == "true",
            r["notes"],
        )
        for r in _read_seed_rows("agent_release.csv")
    ]
    schema = StructType(
        [
            StructField("agent", StringType(), False),
            StructField("first_seen_year", IntegerType(), False),
            StructField("release_date", StringType(), True),
            StructField("release_date_verified", BooleanType(), False),
            StructField("notes", StringType(), True),
        ]
    )
    return spark.createDataFrame(rows, schema=schema)


@dp.materialized_view(
    name=f"{SILVER}.seed_tournament_tier",
    comment="Ordered regex rules classifying VCT tournaments into analytical tiers.",
    table_properties={"quality": "silver", "layer": "seed"},
)
def seed_tournament_tier():
    rows = [
        (int(r["pattern_order"]), r["regex_pattern"], r["tournament_tier"], r["description"])
        for r in _read_seed_rows("tournament_tier.csv")
    ]
    schema = StructType(
        [
            StructField("pattern_order", IntegerType(), False),
            StructField("regex_pattern", StringType(), False),
            StructField("tournament_tier", StringType(), False),
            StructField("description", StringType(), True),
        ]
    )
    return spark.createDataFrame(rows, schema=schema)
