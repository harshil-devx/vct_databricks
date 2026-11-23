"""Reusable PySpark transformation helpers for the VCT Silver layer.

Design principles:
- Never zero-fill missing stats: empty/blank strings become NULL.
- Preserve literal player name "nan" as text (only empty/whitespace is NULL).
- Use ANSI-safe regex-guarded casts so malformed values become NULL and can be
  routed to quarantine rather than crashing the pipeline or being silently dropped.
- Latest-snapshot per year replaces the year slice cleanly on monthly refreshes.
"""

from __future__ import annotations

from pyspark.sql import Column, DataFrame
from pyspark.sql import functions as F

_INT_RE = r"^[+-]?\d+$"
_DOUBLE_RE = r"^[+-]?(\d+(\.\d*)?|\.\d+)$"


def safe_cast_int(col: Column) -> Column:
    """ANSI-safe string -> INT cast returning NULL on non-integer text."""
    return F.when(col.rlike(_INT_RE), col.cast("int")).otherwise(F.lit(None).cast("int"))


def safe_cast_double(col: Column) -> Column:
    """ANSI-safe string -> DOUBLE cast returning NULL on non-numeric text."""
    return F.when(col.rlike(_DOUBLE_RE), col.cast("double")).otherwise(F.lit(None).cast("double"))


def latest_snapshot_by_year(df: DataFrame) -> DataFrame:
    """Keep only rows from max(_snapshot_id) per _year (replace-year-slice semantics)."""
    latest = df.groupBy("_year").agg(F.max("_snapshot_id").alias("_snapshot_id"))
    return df.join(latest, on=["_year", "_snapshot_id"], how="inner")


def latest_snapshot_global(df: DataFrame) -> DataFrame:
    """Keep only rows from max(_snapshot_id) across an unpartitioned all_ids table."""
    latest = df.agg(F.max("_snapshot_id").alias("_snapshot_id"))
    return df.join(latest, on=["_snapshot_id"], how="inner")


def clean_str(col_name: str) -> Column:
    """Trim whitespace and convert empty string to NULL. Preserves literal 'nan'."""
    trimmed = F.trim(F.col(f"`{col_name}`"))
    return F.when(trimmed == "", F.lit(None).cast("string")).otherwise(trimmed)


def parse_int(col_name: str) -> Column:
    """Safely parse an integer string (commas stripped); empty -> NULL."""
    cleaned = F.regexp_replace(clean_str(col_name), ",", "")
    return safe_cast_int(cleaned)


def parse_double(col_name: str) -> Column:
    """Safely parse a floating-point string (commas stripped); empty -> NULL."""
    cleaned = F.regexp_replace(clean_str(col_name), ",", "")
    return safe_cast_double(cleaned)


def parse_pct(col_name: str) -> Column:
    """Parse percentage string like '80%' or '80.5%' into decimal 0.80 / 0.805."""
    cleaned = clean_str(col_name)
    numeric = F.regexp_replace(cleaned, "%", "")
    return safe_cast_double(numeric) / F.lit(100.0)


def parse_credits(col_name: str) -> Column:
    """Parse credit string like '0.3k' -> 300, '21.5k' -> 21500, '400' -> 400."""
    cleaned = F.lower(clean_str(col_name))
    is_k = cleaned.endswith("k")
    num_part = F.when(is_k, F.regexp_replace(cleaned, "k$", "")).otherwise(cleaned)
    val = safe_cast_double(num_part)
    return F.when(val.isNull(), F.lit(None).cast("int")).otherwise(
        F.round(F.when(is_k, val * F.lit(1000.0)).otherwise(val)).cast("int")
    )


def parse_duration_sec(col_name: str) -> Column:
    """Parse duration string 'mm:ss' or 'hh:mm:ss' into total integer seconds."""
    cleaned = clean_str(col_name)
    parts = F.split(cleaned, ":")
    n_parts = F.size(parts)
    p0 = safe_cast_int(parts.getItem(0))
    p1 = safe_cast_int(parts.getItem(1))
    p2 = safe_cast_int(parts.getItem(2))
    mm_ss = p0 * F.lit(60) + p1
    hh_mm_ss = p0 * F.lit(3600) + p1 * F.lit(60) + p2
    return F.when(n_parts == 2, mm_ss).when(n_parts == 3, hh_mm_ss).otherwise(F.lit(None).cast("int"))


def parse_ratio_won(col_name: str) -> Column:
    """Parse 'won/played' string (e.g. '1/5') -> won (INT)."""
    cleaned = clean_str(col_name)
    parts = F.split(cleaned, "/")
    return F.when(F.size(parts) == 2, safe_cast_int(F.trim(parts.getItem(0))))


def parse_ratio_played(col_name: str) -> Column:
    """Parse 'won/played' string (e.g. '1/5') -> played (INT)."""
    cleaned = clean_str(col_name)
    parts = F.split(cleaned, "/")
    return F.when(F.size(parts) == 2, safe_cast_int(F.trim(parts.getItem(1))))


def surrogate_key(prefix: str, *cols: Column) -> Column:
    """Deterministic surrogate key for source-ID-less entities."""
    return F.concat(
        F.lit(f"{prefix}_"),
        F.substring(F.sha2(F.concat_ws("|", *[F.coalesce(c.cast("string"), F.lit("")) for c in cols]), 256), 1, 16),
    )


def documented_gap_reason_expr(tournament_col: str = "tournament") -> Column:
    """Flag China-hosted tournaments where provider documents missing performance/economy stats."""
    return F.when(
        F.col(tournament_col).rlike("(?i)China|Shanghai"),
        F.lit("China-hosted match (stats unavailable per provider)"),
    )


def canonical_win_loss_method_expr(method_col: str = "method") -> Column:
    """Map mirrored win/loss round method names to a 4-value canonical enum."""
    m = F.lower(F.trim(F.col(method_col)))
    return (
        F.when(m.isin("elimination", "eliminated"), F.lit("ELIMINATION"))
        .when(m.isin("defused", "detonated denied"), F.lit("SPIKE_DEFUSED"))
        .when(m.isin("detonated", "failed defused"), F.lit("SPIKE_DETONATED"))
        .when(m.isin("time expiry (no plant)", "time expiry (failed to plant)"), F.lit("TIME_EXPIRY"))
        .otherwise(F.lit("UNKNOWN"))
    )
