# ADR 005: Prefix-Based Schema Isolation (`dev_` vs. `prod`)

- **Status:** Accepted
- **Context:** Databricks Free Edition restricts catalog/workspace provisioning and allows only one active pipeline per type, while requiring strict isolation between `dev` and `prod` tables.
- **Decision:** Share the read-only landing volume (`final_project.vct_landing.raw`) across targets and isolate medallion schemas via `${var.env_prefix}` (`dev_` in `dev` -> `dev_vct_bronze/silver/gold`; `""` in `prod` -> `vct_bronze/silver/gold`), publishing all three layers from one multi-schema SDP pipeline.
- **Consequences:** Zero duplicated landing storage, clean `dev`/`prod` separation in Unity Catalog, and full compliance with Free Edition pipeline limits.
- **Alternatives rejected:** Separate catalogs per environment or separate pipelines per medallion layer (blocked by Free Edition limits).
