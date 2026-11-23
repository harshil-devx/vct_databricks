# ADR 002: All-STRING Bronze with Auto Loader Rescue Mode

- **Status:** Accepted
- **Context:** Raw CSVs contain formatted strings (`"80%"`, `"0.3k"`, `"1/5"`), literal `"nan"` player names, and empty strings representing meaningful `NULL`s. Typing in Bronze risks corrupting values or crashing ingestion on schema drift.
- **Decision:** Ingest all 26 Bronze tables via Auto Loader with `cloudFiles.inferColumnTypes = false` (all `STRING`) and `cloudFiles.schemaEvolutionMode = rescue`, preserving `_rescued_data`. For `all_ids` tables that arrive in later snapshots, supply explicit all-`STRING` schemas including `_rescued_data` so empty globs initialize cleanly.
- **Consequences:** Bronze is lossless and resilient to upstream drift; `_rescued_data` is monitored in `data_quality_checks` (currently 0 rescued rows across all 152 snapshot groups).
- **Alternatives rejected:** Schema inference in Bronze (turns literal `"nan"` into `NULL` and fails on mixed string formats).
