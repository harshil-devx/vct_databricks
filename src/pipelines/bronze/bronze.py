"""Config-driven Bronze layer: 26 Auto Loader streaming tables (21 yearly + 5 all_ids).

Rules:
- All source columns stay STRING (no typing/cleaning in bronze).
- Column headers are renamed to Delta-safe snake_case; _rescued_data is preserved.
- Audit columns (_source_file, _file_modified_at, _ingested_at) are extracted from
  _metadata in the same projection as the rename, followed by _snapshot_id and _year.
"""

from pyspark import pipelines as dp
from pyspark.sql import functions as F
from pyspark.sql.types import StringType, StructField, StructType

from pipelines.bronze.bronze_config import BRONZE_TABLES, BronzeTableSpec, snake_names

# Set per target in resources/vct_medallion.pipeline.yml
BRONZE = spark.conf.get("vct.bronze_schema")  # e.g. final_project.dev_vct_bronze
LANDING = spark.conf.get("vct.landing_root")  # e.g. /Volumes/final_project/vct_landing/raw/vct


def _build_string_schema(raw_columns: tuple[str, ...]) -> StructType:
    """Build an explicit all-STRING schema including _rescued_data for Auto Loader."""
    fields = [StructField(col, StringType(), True) for col in raw_columns]
    fields.append(StructField("_rescued_data", StringType(), True))
    return StructType(fields)


def register_bronze_table(spec: BronzeTableSpec) -> None:
    """Register one Auto Loader streaming table in the pipeline DAG."""
    glob_path = f"{LANDING}/*/vct_*/{spec.rel_path}" if spec.is_yearly else f"{LANDING}/*/{spec.rel_path}"

    @dp.table(
        name=f"{BRONZE}.{spec.table_name}",
        comment=spec.comment,
        table_properties={"quality": "bronze"},
    )
    def _bronze_table():
        reader = (
            spark.readStream.format("cloudFiles")
            .option("cloudFiles.format", "csv")
            .option("cloudFiles.inferColumnTypes", "false")
            .option("cloudFiles.schemaEvolutionMode", "rescue")
            .option("header", "true")
            .option("escape", '"')
        )
        if spec.raw_columns is not None:
            reader = reader.schema(_build_string_schema(spec.raw_columns))

        df = reader.load(glob_path)

        # _metadata is only resolvable in the first projection on the file source.
        data_cols = [F.col(f"`{c}`").alias(n) for c, n in zip(df.columns, snake_names(df.columns), strict=True)]
        audit_cols = [
            F.col("_metadata.file_path").alias("_source_file"),
            F.col("_metadata.file_modification_time").alias("_file_modified_at"),
            F.current_timestamp().alias("_ingested_at"),
        ]
        df = df.select(*data_cols, *audit_cols)

        year = F.regexp_extract("_source_file", r"/vct_(\d{4})/", 1)
        return df.withColumn(
            "_snapshot_id",
            F.regexp_extract("_source_file", r"/vct/([^/]+)/", 1),
        ).withColumn(
            "_year",
            F.when(year != "", year.cast("int")),
        )


for _spec in BRONZE_TABLES:
    register_bronze_table(_spec)
