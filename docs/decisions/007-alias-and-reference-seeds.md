# ADR 007: Version-Controlled Reference Seeds in `seeds/`

- **Status:** Accepted
- **Context:** Resolving 2025 `eco_rounds` legacy tournament names, tracking unverified/unresolved team identities (`Mega Minors`, `Kiwoom DRX`), filtering pre-release agent meta rows, and classifying tournament tiers require curated domain mappings that should be reviewable in Git PRs.
- **Decision:** Store 4 small CSV seeds (`tournament_alias.csv`, `team_alias.csv`, `agent_release.csv`, `tournament_tier.csv`) under `seeds/`, deploy them with the bundle, and load them into Silver via `vct.seeds_dir: ${workspace.file_path}/seeds`.
- **Consequences:** All domain overrides are auditable in Git history, unverified entries carry explicit `status` / `verified` flags, and deployments are reproducible across `dev` and `prod`.
- **Alternatives rejected:** Hardcoding giant `WHEN` chains inside PySpark files or uploading ad-hoc CSVs outside version control.
