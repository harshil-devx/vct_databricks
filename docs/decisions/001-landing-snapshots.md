# ADR 001: Immutable Snapshot Folders in Landing

- **Status:** Accepted
- **Context:** The Kaggle VCT dataset is updated monthly by republishing complete CSV files for the active year and `all_ids/`. Overwriting files in place breaks Auto Loader incremental tracking and makes historical debugging impossible.
- **Decision:** Land each drop in an immutable snapshot directory `/Volumes/final_project/vct_landing/raw/vct/<snapshot_id>/vct_<year>/...` (and `.../<snapshot_id>/all_ids/...`), simulated here with `s01_2021`..`s05_2025`, synthetic 80% `s06_2026_partial`, and `s07_2026_full`.
- **Consequences:** Auto Loader processes each file exactly once without `allowOverwrites`, `_snapshot_id` is deterministically extracted from `_metadata.file_path`, and every historical drop remains auditable in Bronze.
- **Alternatives rejected:** Overwriting CSVs in place under `vct_2026/` (breaks incremental ingestion and loses audit history).
