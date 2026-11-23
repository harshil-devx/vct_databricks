# Bronze: matches_overview
# Raw CSVs from the landing volume -> one streaming table, all columns as strings.
import re

from pyspark.sql import functions as F

try:
    from pyspark import pipelines as dp
except ImportError:  # older runtimes expose the same API as `dlt`
    import dlt as dp

# Set per target in resources/vct_medallion.pipeline.yml
BRONZE = spark.conf.get("vct.bronze_schema")    # e.g. final_project.dev_vct_bronze
LANDING = spark.conf.get("vct.landing_root")    # e.g. /Volumes/final_project/vct_landing/raw/vct


def to_snake(name: str) -> str:
    s = name.strip().lower().replace("%", " pct ")
    s = re.sub(r"[^a-z0-9]+", "_", s).strip("_")
    return s or "col"


def snake_names(columns):
    new = [c if c == "_rescued_data" else to_snake(c) for c in columns]
    dupes = sorted({n for n in new if new.count(n) > 1})
    if dupes:
        raise ValueError(f"column names collide after snake_case: {dupes}")
    return new


@dp.table(
    name=f"{BRONZE}.matches_overview",
    comment="Raw player-per-map stats from matches/overview.csv. All columns are strings.",
    table_properties={"quality": "bronze"},
)
def matches_overview():
    df = (
        spark.readStream.format("cloudFiles")
        .option("cloudFiles.format", "csv")
        .option("cloudFiles.inferColumnTypes", "false")
        .option("cloudFiles.schemaEvolutionMode", "rescue")
        .option("header", "true")
        .option("escape", '"')
        .load(f"{LANDING}/*/vct_*/matches/overview.csv")
    )

    # _metadata is only resolvable in this first projection, so build audit columns here.
    data_cols = [F.col(f"`{c}`").alias(n) for c, n in zip(df.columns, snake_names(df.columns))]
    audit_cols = [
        F.col("_metadata.file_path").alias("_source_file"),
        F.col("_metadata.file_modification_time").alias("_file_modified_at"),
        F.current_timestamp().alias("_ingested_at"),
    ]
    df = df.select(*data_cols, *audit_cols)

    year = F.regexp_extract("_source_file", r"/vct_(\d{4})/", 1)
    return (
        df.withColumn("_snapshot_id", F.regexp_extract("_source_file", r"/vct/([^/]+)/", 1))
        .withColumn("_year", F.when(year != "", year.cast("int")))
    )