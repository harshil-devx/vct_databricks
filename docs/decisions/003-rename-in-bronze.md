# ADR 003: Rename Headers to `snake_case` in Bronze

- **Status:** Accepted
- **Context:** Delta Lake rejects raw VCT CSV headers containing `%`, commas, and parentheses (e.g., `"Kill, Assist, Trade, Survive %"`, `"Kills - Deaths (KD)"`).
- **Decision:** Normalize column headers to Delta-safe `snake_case` in Bronze (`%` -> `pct`, non-alphanumeric runs -> `_`) via pure-Python `to_snake` / `snake_names` with a collision guard, while keeping `_rescued_data` verbatim and leaving all cell values completely untouched.
- **Consequences:** Bronze tables write cleanly without enabling Delta column-mapping workarounds, and naming logic is unit-tested in 50ms via `pytest`.
- **Alternatives rejected:** Enabling `delta.columnMapping.mode = name` to preserve messy headers (pushes backtick-quoting burden onto every downstream SQL/PySpark query).
