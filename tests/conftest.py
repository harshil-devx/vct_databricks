"""Pytest configuration with lazy Databricks Connect fixtures.

Pure-Python unit tests run locally without starting a remote SparkSession.
Tests that explicitly request the `spark` or `load_fixture` fixtures connect
to Databricks serverless compute on demand.
"""

import csv
import json
import os
import pathlib
import sys

import pytest


def _enable_fallback_compute() -> None:
    """Enable serverless compute if no compute is specified."""
    from databricks.sdk import WorkspaceClient

    conf = WorkspaceClient().config
    if conf.serverless_compute_id or conf.cluster_id or os.environ.get("SPARK_REMOTE"):
        return

    print("no compute specified, falling back to serverless compute", file=sys.stderr)
    os.environ["DATABRICKS_SERVERLESS_COMPUTE_ID"] = "auto"


@pytest.fixture(scope="session")
def spark():
    """Provide a Databricks Connect SparkSession lazily when requested by a test."""
    from databricks.connect import DatabricksSession

    _enable_fallback_compute()
    if hasattr(DatabricksSession.builder, "validateSession"):
        return DatabricksSession.builder.validateSession().getOrCreate()
    return DatabricksSession.builder.getOrCreate()


@pytest.fixture()
def load_fixture(spark):
    """Provide a callable to load JSON or CSV from the fixtures/ directory."""

    def _loader(filename: str):
        path = pathlib.Path(__file__).parent.parent / "fixtures" / filename
        suffix = path.suffix.lower()
        if suffix == ".json":
            rows = json.loads(path.read_text(encoding="utf-8"))
            return spark.createDataFrame(rows)
        if suffix == ".csv":
            with path.open(newline="", encoding="utf-8") as f:
                rows = list(csv.DictReader(f))
            return spark.createDataFrame(rows)
        raise ValueError(f"Unsupported fixture type for: {filename}")

    return _loader
