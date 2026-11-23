# Engineering Learning Log

Key technical lessons, runtime discoveries, and architectural takeaways from building the VCT Lakehouse pipeline end-to-end on Databricks Free Edition.

## Phase A — Bronze & Auto Loader
1. **Modern SDP API (`from pyspark import pipelines as dp`):** Verified directly on Free Edition serverless runtime. Legacy `import dlt` is unnecessary.
2. **Decorator return objects are not plain functions:** `@dp.table(...)` returns a `DatasetOrExpectationDecoratorResult` whose `__name__` attribute is read-only. Passing explicit `name=f"{BRONZE}.{spec.table_name}"` is cleaner and avoids mutating decorator wrappers.
3. **Empty-directory Auto Loader bootstrap:** Auto Loader cannot infer a CSV schema when a glob matches 0 files (as happened for `all_ids/*` during snapshots `s01`–`s06`). Providing an explicit all-`STRING` `StructType` including `_rescued_data STRING` lets empty tables initialize cleanly, ingest incrementally on `s07`, and still rescue unexpected columns.
4. **Pure-Python config separation & lazy `conftest.py`:** The default DABs `conftest.py` eagerly launches a remote Databricks Connect session in `pytest_configure`, which slows local tests and breaks PR CI without workspace creds. Making the `spark` fixture lazy and isolating `bronze_config.py` cut unit-test runtime to 0.05s.
5. **Reconciliation discipline:** Comparing all 152 table-snapshot combinations against exact local CSV row counts (`scripts/reconcile_bronze.py`) gave 100% proof of incremental Auto Loader behavior before writing a single line of Silver.

## Phase B — Silver, Seeds & Quality Expectations
1. **No `__file__` in SDP pipeline cells:** Databricks SDP executes pipeline library files like notebook cells without `__file__`. Injecting `vct.seeds_dir: ${workspace.file_path}/seeds` through `vct_medallion.pipeline.yml` keeps seeds version-controlled in Git and portable across `dev` and `prod`.
2. **ANSI-safe typing without `F.try_cast`:** Serverless Spark defaults to ANSI mode (casting bad text throws an exception) and did not expose `F.try_cast` on `pyspark.sql.functions`. Regex-guarded casts (`safe_cast_int` / `safe_cast_double`) are ANSI-safe, version-independent, and let `quarantine_silver_records` audit parse failures cleanly.
3. **Verify schemas instead of assuming symmetry:** Yearly `ids_tournament_stage_matches_games` lacked `match_type_id`, whereas `all_ids_tournament_stage_match_types` had it. Enriching yearly lookups via a left join to `dim_stage_match_type` kept every downstream fact consistent.
4. **Verify hypotheses on live data:** Querying `matches_rounds_kills` on Bronze confirmed `kill_type` has only 9 highlight tags (`2k`–`5k`, `1v1`–`1v5`), preventing a major analytical bug where someone might sum it as total match kills.

## Phase C — Gold Star Schema & Marts
1. **Never import one SDP library file from another:** Importing `col_doc` from `gold_dimensions.py` inside `gold_facts.py` failed with `NameError: name 'spark' is not defined` because top-level `spark.conf.get(...)` ran outside SDP's cell context. Shared utilities belong in non-pipeline helper modules (`gold_utils.py`).
2. **Column comments via Spark metadata:** `.alias(name, metadata={"comment": "..."})` propagated **199/199** column comments into Unity Catalog without maintaining duplicate DDL schemas.
3. **Derive meta from ground truth:** Computing `agent_meta_by_tournament` directly from non-rollup `fct_player_game_stats` filtered by `year >= first_seen_year` eliminated `vlr.gg`'s pre-release `0%` artifacts.

## Phase D — Lakeflow Jobs & DABs
1. **Pipeline expectations vs. Job quality gates:** In triggered SDP pipelines, a failing private check MV rolls back only its own flow. Pairing pipeline expectations with a downstream `data_quality_checks` job task ensures the workflow fails loudly on any cross-layer invariant violation.
2. **Safe production defaults:** Parameterizing `schedule_pause_status` (default `PAUSED`) ensures deploying `prod` never activates a schedule prematurely.

## Phase E — CI/CD on Free Edition
1. **Free Edition auth realities:** `databricks tokens list` confirmed PATs are supported on Free Edition, while the absence of an Account Console prevents configuring OAuth M2M / OIDC federation. Documenting the PAT setup for Free Edition alongside the OIDC upgrade path for Enterprise workspaces makes the repo honest and production-ready.
