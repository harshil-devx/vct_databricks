# VCT Dataset Profile Summary (2021–2026)

Empirical findings from profiling all 131 CSVs (~1.36 GB, 21 yearly file types × 6 years + 5 `all_ids` files) that directly shaped the Bronze, Silver, and Gold designs.

## 1. Identity & Key Structure
- **No ID columns in fact CSVs:** Fact files contain only display names (`Tournament`, `Stage`, `Match Type`, `Match Name`, `Map`, `Team`, `Player`). All IDs live in `ids/` (yearly) and `all_ids/` (cross-year).
- **Yearly 1:1 vs. cross-year drift:** Within any single year, `player -> player_id` and `team -> team_id` map 1:1. Across years, 224 player names map to multiple Player IDs, while 15 Player IDs and 5 Team IDs have multiple historical names (renames). Silver therefore resolves names using yearly `ids_*` tables, conforms on ID across years, and stores name-history arrays.
- **Ambiguous game keys:** `(Tournament, Stage, Match Type, Match Name, Map)` resolves 99.77–100% of games. Exactly **19 keys in 2021** and **4 keys in 2022** map to two distinct Game IDs. These are isolated in `quarantine_ambiguous_games` and `quarantine_silver_records` to prevent join fan-out.
- **ID-less players & unverified teams:**
  - ~410 players in 2021 lack a source Player ID (documented gap) -> deterministic surrogate key `SK_PLAYER_*` with `has_source_id = false`.
  - `"Mega Minors"` (2025–2026) only has an ID (`3788`) in 2021 `ids_teams` -> provisionally mapped in `seeds/team_alias.csv` with `status = unverified`.
  - `"Kiwoom DRX"` (~272 rows in 2026) has no ID in any file -> `status = unresolved`, receives surrogate key `SK_TEAM_*`.
- **`all_ids` superset for match types:** `all_ids/all_tournaments_stages_match_types_ids.csv` contains extra rows missing from yearly files (2021 +1, 2025 +5 showmatches, 2026 +12 Masters London) and carries `match_type_id` (absent from yearly `ids_tournament_stage_matches_games`). Used as the authoritative source for `dim_tournament` and `dim_stage_match_type`.

## 2. Mixed Grains & Structural Quirks
- **`"All Maps"` rollups:** Present in `overview`, `kills`, `kills_stats`, and `eco_stats`. Separated from map-level facts in Silver (`is_map_rollup` flag) and excluded from all Gold additive marts.
- **`"All Stages"` / `"All Match Types"` rollups:** Present in `agents/*` and `players_stats` with legitimately `NULL` IDs. Flagged via `is_tournament_rollup`.
- **2025 `eco_rounds` legacy names:** Uses `"Champions Tour 2025: <Region> <Event>"` instead of `"VCT 2025: ..."` (6 events) plus `"Champions Tour 2025: Masters Bangkok"` instead of `"Valorant Masters Bangkok 2025"`. Normalized via `seeds/tournament_alias.csv` before game resolution, lifting 2025 `fct_economy_round` resolution to **100.00%**.
- **Mirrored round outcomes:** `win_loss_methods_round_number` stores two mirrored rows per round with exact 1:1 symmetric counts across 4 pairs (`Elimination`/`Eliminated`: 393,133 each; `Defused`/`Detonated Denied`: 109,644; `Detonated`/`Failed Defused`: 40,916; `Time Expiry (No Plant)`/`Time Expiry (Failed to Plant)`: 12,943). Standardized into a 4-value canonical enum.
- **Long/multi-view formats:** `eco_stats` has 5 rows per team-map (`Pistol Won`, `Eco (won)`, `$ (won)`, `$$ (won)`, `$$$ (won)` — pivoted wide in Silver); `kills.csv` has 3 views per player pair (`All Kills`, `First Kills`, `Op Kills` — pivoted wide in Silver).
- **`matches_rounds_kills` verified finding:** Profiling `kill_type` on Bronze proved it contains **only** 9 highlight tags (`2k`, `3k`, `4k`, `5k`, `1v1`–`1v5`). It is NOT a complete kill log; modelled as `fct_highlight_round_kill` with an explicit warning.
- **Pre-release agent noise in `agents_pick_rates`:** `vlr.gg` emits `0%` rows for newer agents (e.g. `clove`) in 2021 files. Gold `agent_meta_by_tournament` computes pick rates directly from `fct_player_game_stats` and filters `year >= dim_agent.first_seen_year`.

## 3. Data Gaps & Special Values
- **Documented gaps:** China-hosted tournaments (regex `(?i)China|Shanghai`, including Masters Shanghai 2024) lack performance/economy stats; `Loadout Value` is empty from Masters Toronto 2025 onward; literal player `"nan"` (`player_id = 10207`) is a real handle; `"TBD"` is a placeholder team; 62 matches end in a draw (`winner_team_id IS NULL`).
- **Undocumented gaps (verified via Silver coverage flags):**
  - 2021: only **62.20%** of games have performance stats and **62.24%** have economy stats (entire Challengers events missing).
  - 2025: **78.23%** have performance stats but only **46.44%** have economy stats (`eco_rounds` missing for Stage 2 and Champions 2025).
- **Season volume skew:** 2021–2022 hold ~85% of all games (155 `Challengers_Qualifier` tournaments vs. 10–15 franchised/international tournaments per year in 2023+), making `tournament_tier` essential in Gold.
- **Header compatibility:** Raw headers contain `%`, commas, and parentheses rejected by Delta; all 26 header sets convert to `snake_case` with 0 collisions and 0 schema drift across 2021–2026.
