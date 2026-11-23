"""Shared pure helper for attaching Unity Catalog column comments in Gold."""

from __future__ import annotations

from pyspark.sql import Column
from pyspark.sql import functions as F


def col_doc(expr: Column | str, name: str, comment: str) -> Column:
    """Attach a Unity Catalog column comment via Spark column metadata."""
    c = F.col(expr) if isinstance(expr, str) else expr
    return c.alias(name, metadata={"comment": comment})
