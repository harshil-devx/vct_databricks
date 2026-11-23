# ADR 006: Explicit Rollup Flags and Single-Grain Gold Facts

- **Status:** Accepted
- **Context:** Source CSVs mix grains in the same file: `Map = "All Maps"` match rollups appear alongside map rows, and `Stage = "All Stages"` / `Match Type = "All Match Types"` tournament rollups appear with `NULL` IDs. Summing mixed grains silently double-counts every metric.
- **Decision:** In Silver, separate `"All Maps"` overview rows into `fct_player_match_rollup_stats` (`is_map_rollup = true`), exclude `"All Maps"` from map-grain facts, and flag tournament rollups via `is_tournament_rollup`. In Gold, enforce strictly one grain per table so all additive measures derive exclusively from non-rollup rows.
- **Consequences:** Analysts cannot accidentally double-count kills, maps, or rounds when aggregating Gold tables.
- **Alternatives rejected:** Keeping mixed grains in Gold and relying on analysts to remember `WHERE map != 'All Maps'`.
