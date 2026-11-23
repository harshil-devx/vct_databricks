# VCT Gold Layer Data Dictionary

This document describes the analyst-ready **Gold** schema (`final_project.vct_gold` in `prod`, `final_project.dev_vct_gold` in `dev`) for the Valorant Champions Tour (VCT) 2021–2026 dataset.

## Analytical Guardrails
- **One grain per table:** Gold facts and marts strictly exclude mixed-grain rollup rows (`Map = "All Maps"`, `Stage = "All Stages"`, `Match Type = "All Match Types"`). All additive measures (kills, deaths, maps won, rounds won) come from map-level or match-level non-rollup facts.
- **No zero-filling of missing stats:** Provider-documented gaps (e.g., China-hosted tournaments such as Masters Shanghai 2024, empty Loadout Value from Masters Toronto 2025 onward) and undocumented gaps (2021 Challengers performance/economy, 2025 Stage 2 & Champions `eco_rounds`) remain `NULL`. Averages ignore `NULL`s and carry explicit `*_coverage_pct` columns (`games_with_stats / games_played`).
- **Tournament tiers:** Because 2021–2022 contain ~85% of all games (mostly Challengers and open qualifiers) while 2023+ contain 10–15 franchised/international tournaments per year, every Gold fact and mart includes `tournament_tier` so analysts can filter cleanly (e.g. `International_Champions`, `International_Masters`, `Regional_League`).
- **Entity identity:** Players and teams are conformed by source ID across seasons (retaining name history arrays). ~410 ID-less 2021 players receive deterministic surrogate keys (`SK_PLAYER_*`, `has_source_id = false`). Literal player `"nan"` is preserved as text (`player_id = 10207`). Placeholder team `"TBD"` is flagged (`is_placeholder_tbd = true`) and excluded from team marts.

---

## Star Schema Dimensions

### `dim_tournament`
- **Grain:** 1 row per `tournament_id`
- **Columns:** `tournament_id` (PK), `tournament_name`, `year`, `tournament_tier`, `documented_gap_reason`

### `dim_team`
- **Grain:** 1 row per `team_id`
- **Columns:** `team_id` (PK), `canonical_team_name`, `team_name_history` (`ARRAY<STRING>`), `abbreviated_names` (`ARRAY<STRING>`), `has_source_id`, `is_placeholder_tbd`, `alias_status` (`verified` / `unverified` / `unresolved`), `first_seen_year`, `last_seen_year`

### `dim_player`
- **Grain:** 1 row per `player_id`
- **Columns:** `player_id` (PK), `canonical_player_name`, `player_name_history` (`ARRAY<STRING>`), `has_source_id`, `first_seen_year`, `last_seen_year`

### `dim_agent`
- **Grain:** 1 row per lowercase `agent`
- **Columns:** `agent` (PK), `first_seen_year` (verified from `matches_overview`), `release_date`, `release_date_verified`

### `dim_map`
- **Grain:** 1 row per `map_name` (excludes `"All Maps"`)
- **Columns:** `map_name` (PK), `first_seen_year`, `last_seen_year`

---

## Star Schema Core Facts

### `fct_game_scores`
- **Grain:** 1 row per `game_id` (one map played in a match)
- **Columns:** `game_id` (PK), `match_id`, `tournament_id`, `year`, `tournament_tier`, `map_name`, `team_a_id`, `team_b_id`, `winner_team_id` (`NULL` on draw), `team_a_score`, `team_a_attacker_score`, `team_a_defender_score`, `team_a_overtime_score` (`0` when `is_overtime = false`), `team_b_score`, `team_b_attacker_score`, `team_b_defender_score`, `team_b_overtime_score`, `is_overtime`, `is_draw`, `is_showmatch`, `duration_seconds`, `has_performance_stats`, `has_economy_stats`, `documented_gap_reason`

### `fct_player_game_stats`
- **Grain:** 1 row per `(game_id, player_id)` at `side = 'both'` (excludes `"All Maps"` rollups)
- **Columns:** `game_id`, `match_id`, `tournament_id`, `year`, `tournament_tier`, `map_name`, `player_id`, `team_id`, `agent`, `rating`, `acs`, `kills`, `deaths`, `assists`, `kd_diff`, `kast_pct`, `adr`, `hs_pct`, `first_kills`, `first_deaths`, `fk_fd_diff`, `is_showmatch`, `documented_gap_reason`

---

## Analyst Marts

### 1. `match_results_base`
- **Grain:** 1 row per `match_id`
- **Purpose:** Flat match-level results table for standings, brackets, and win/loss timelines.
- **Key columns:** `match_id`, `year`, `tournament_id`, `tournament_name`, `tournament_tier`, `stage`, `match_type`, `match_name`, `team_a_id`, `team_a_name`, `team_b_id`, `team_b_name`, `team_a_score`, `team_b_score`, `maps_played`, `winner_team_id` (`NULL` on draw), `winner_team_name`, `is_draw`, `is_showmatch`, `documented_gap_reason`

### 2. `player_career_summary`
- **Grain:** 1 row per `player_id` (excludes showmatches)
- **Purpose:** All-time career leaderboards and player profiles with explicit rating coverage.
- **Key columns:** `player_id`, `canonical_player_name`, `has_source_id`, `first_seen_year`, `last_seen_year`, `most_played_agent`, `games_played`, `matches_played`, `games_with_rating`, `rating_coverage_pct`, `total_kills`, `total_deaths`, `total_assists`, `career_kd_ratio`, `total_first_kills`, `total_first_deaths`, `avg_rating`, `avg_acs`, `avg_adr`, `avg_hs_pct`, `avg_kast_pct`, `games_with_documented_gap`

### 3. `player_season_form`
- **Grain:** 1 row per `(player_id, year, tournament_tier)` (excludes showmatches)
- **Purpose:** Season-by-season player form segmented by competition tier.
- **Key columns:** `player_id`, `canonical_player_name`, `year`, `tournament_tier`, `games_played`, `games_with_rating`, `rating_coverage_pct`, `total_kills`, `total_deaths`, `total_assists`, `kd_ratio`, `avg_rating`, `avg_acs`, `avg_adr`, `avg_hs_pct`, `avg_kast_pct`, `games_with_documented_gap`

### 4. `team_results_timeline`
- **Grain:** 1 row per `(team_id, year, tournament_id)` (excludes `"TBD"` and showmatches)
- **Purpose:** Team performance across tournaments over time.
- **Key columns:** `team_id`, `canonical_team_name`, `alias_status`, `year`, `tournament_id`, `tournament_name`, `tournament_tier`, `matches_played`, `matches_won`, `matches_lost`, `matches_drawn`, `match_win_rate`, `maps_won`, `maps_lost`, `documented_gap_reason`

### 5. `team_map_pool_winrate`
- **Grain:** 1 row per `(team_id, map_name, year, tournament_tier)` (excludes `"TBD"` and showmatches)
- **Purpose:** Team map-veto scouting and side strength by map and season.
- **Key columns:** `team_id`, `canonical_team_name`, `map_name`, `year`, `tournament_tier`, `games_played`, `games_won`, `games_lost`, `games_drawn`, `map_win_rate`, `avg_round_diff`, `avg_atk_rounds_won`, `avg_def_rounds_won`, `games_with_documented_gap`

### 6. `agent_meta_by_tournament`
- **Grain:** 1 row per `(tournament_id, map_name, agent)` (excludes showmatches and pre-release tournaments where `year < agent.first_seen_year`)
- **Purpose:** Accurate agent meta pick rates computed from actual team compositions (`times_picked / (2 * games_played)`), avoiding vlr.gg's pre-release `0%` artifacts.
- **Key columns:** `tournament_id`, `tournament_name`, `year`, `tournament_tier`, `map_name`, `agent`, `agent_first_seen_year`, `games_played`, `times_picked`, `pick_rate`, `documented_gap_reason`

### 7. `map_side_balance`
- **Grain:** 1 row per `(map_name, year, tournament_tier)` (excludes showmatches)
- **Purpose:** Attack vs. defense round balance, overtime rates, and match duration trends by map.
- **Key columns:** `map_name`, `year`, `tournament_tier`, `games_played`, `total_attacker_rounds_won`, `total_defender_rounds_won`, `attacker_win_rate`, `defender_win_rate`, `overtime_rate`, `avg_duration_seconds`, `duration_coverage_pct`, `games_with_documented_gap`

### 8. `economy_efficiency_by_team`
- **Grain:** 1 row per `(team_id, year, tournament_tier)` (excludes `"TBD"` and showmatches)
- **Purpose:** Pistol, eco, semi-eco (`$`), semi-buy (`$$`), and full-buy (`$$$`) conversion efficiency with explicit `economy_coverage_pct`.
- **Key columns:** `team_id`, `canonical_team_name`, `year`, `tournament_tier`, `games_played`, `games_with_economy_stats`, `economy_coverage_pct`, `pistol_rounds_won`, `pistol_win_rate`, `eco_win_rate`, `semi_eco_win_rate`, `semi_buy_win_rate`, `full_buy_win_rate`, `games_with_documented_gap`
