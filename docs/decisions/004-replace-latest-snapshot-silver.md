# ADR 004: Replace-Latest-Snapshot per Year in Silver

- **Status:** Accepted
- **Context:** Monthly refreshes deliver complete CSV snapshots for the active year (`s06_2026_partial` followed by `s07_2026_full`), and many fact tables lack reliable natural primary keys before ID resolution.
- **Decision:** Implement Silver tables as `@dp.materialized_view`s that filter Bronze to `max(_snapshot_id)` per `_year` (and global `max(_snapshot_id)` for `all_ids`).
- **Consequences:** A new snapshot cleanly replaces that year's slice with zero duplicate rows (verified: 0 `s06_2026_partial` rows remain in Silver after `s07_2026_full`).
- **Alternatives rejected:** Append-only streaming tables in Silver (would duplicate every 2026 row across snapshots) or complex `MERGE` logic on unreliable natural keys.
