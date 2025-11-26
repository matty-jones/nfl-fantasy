# PROGRESS

- 2025-11-26: Noticed `SchemaError: type Int32 is incompatible with expected type Null` while running `python main.py --seasons 2025 -o week13_lineup --player "Quinshon Judkins, Kenneth Gainwell" --team "CLE, SF49"`. Need to trace where Polars expects null columns (likely when combining player and D/ST outputs) and ensure column types align before concatenation.
- 2025-11-26: Added dtype-aware fillers when aligning player and D/ST columns before concatenation, preventing Null-only columns from conflicting with numeric types. Re-ran the CLI command and confirmed it now exits cleanly while still saving outputs.
- 2025-11-26: Sorted the combined output by `season`/`week` so D/ST rows also appear chronologically.
- 2025-11-26: Adjusted the concatenation logic to sort players and D/ST tables separately before merging, so the output keeps players grouped first and each section stays chronological.

