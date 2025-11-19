# NFL Fantasy League Data Processor

A Python tool to process NFL statistics and calculate fantasy points based on Porch Crew scoring rules. Generates CSV files with player statistics and fantasy points for use in decision-making.

## Installation

1. Ensure you have Python 3.8+ installed.

2. Activate the virtual environment:
```bash
source .venv/bin/activate
```

3. Install dependencies:
```bash
pip install -r requirements.txt
```

## Weekly Usage

### Basic Usage

Process all weeks for the default season (2025):
```bash
python main.py
```

### Process Specific Week

To process data for a specific week:
```bash
python main.py --week 1
```

### Process Multiple Seasons

To process multiple seasons:
```bash
python main.py --seasons "2024,2025"
```

### Custom Output Directory

To specify a custom output directory:
```bash
python main.py --output-dir my_output
```

### Player Search

To search for a specific player and generate their statistics:
```bash
python main.py --player "Patrick Mahomes"
```

To search for multiple players:
```bash
python main.py --player "Patrick Mahomes,Travis Kelce"
```

To display player statistics in the console:
```bash
python main.py --player "Patrick Mahomes" --display
```

### Week Range Support

The `--week` option supports multiple formats:
- Single week: `--week 11`
- Comma-separated: `--week "8,9,10"`
- Range: `--week "8-10"`
- Mixed: `--week "8,9,11-13"`

### Summary Statistics

Generate summary statistics for specific players:
```bash
python main.py --summary --player "Patrick Mahomes,Travis Kelce" --week "8-10"
```

This generates a summary CSV file with aggregated statistics across the specified weeks.

### Combined Options

Example: Process week 5 of 2025 season and save to custom directory:
```bash
python main.py --seasons "2025" --week 5 --output-dir weekly_stats
```

## Output Files

The tool generates separate CSV files for each position:

- `qb_stats.csv` - Quarterback statistics and fantasy points
- `rb_stats.csv` - Running back statistics and fantasy points
- `wr_te_stats.csv` - Wide receiver and tight end statistics and fantasy points
- `k_stats.csv` - Kicker statistics and fantasy points
- `dst_stats.csv` - Defense/Special Teams statistics and fantasy points

If a week filter is specified, filenames will include the week number (e.g., `qb_stats_week_1.csv`).

Each file includes:
- Week number
- Team
- Player name (or team for D/ST)
- All raw statistics used in scoring calculations
- Calculated fantasy score

## Scoring Rules

The tool implements Porch Crew fantasy scoring rules:

### Offensive Players (QB, RB, WR, TE)
- Passing: 0.04 points per yard, 6 points per TD, -3 per interception, 2 points per 2PT conversion
- Rushing: 0.10 points per yard, 6 points per TD, 2 points per 2PT conversion
- Receiving: 0.10 points per yard, 1 point per reception, 6 points per TD, 2 points per 2PT conversion
- Long TD bonuses: +2 points for 40+ yard TDs, +3 points for 50+ yard TDs
- Fumbles: -2 points per fumble lost
- Special teams/defensive TDs: 6 points

### Kickers
- Extra points: 1 point
- Field goals: 3 points (0-39 yards), 4 points (40-49), 5 points (50-59), 6 points (60+)
- Missed field goals: -1 point (all distances), additional -3 for 0-39 yards, additional -2 for 40-49 yards

### Defense/Special Teams
- Sacks, interceptions, fumbles recovered, blocked kicks: 2 points each
- Safeties: 5 points
- Defensive TDs: 6 points
- Points/yards allowed: Bucket-based scoring (see scoring.py for details)

## Project Structure

- `main.py` - Main entry point with CLI using Typer
- `scoring.py` - Fantasy scoring calculation functions for all positions
- `data_loader.py` - Functions to load data from nflreadpy
- `data_processor.py` - Data processing, joining logic, and player/team matching
- `output.py` - Output generation and file saving
- `summary.py` - Summary statistics generation
- `config.py` - Configuration settings (default seasons, output directory)
- `fantasy.py` - Legacy scoring functions (reference implementation)

## Features

- **Fuzzy Player Matching**: Search for players by name with automatic fuzzy matching
- **D/ST Support**: Search for defense/special teams by team name
- **Long TD Bonuses**: Automatically calculates 40+ and 50+ yard touchdown bonuses from play-by-play data
- **Flexible Week Filtering**: Support for single weeks, ranges, and comma-separated lists
- **Summary Statistics**: Generate aggregated statistics across multiple weeks for specific players
- **Position-Specific Outputs**: Separate CSV files for QB, RB, WR/TE, K, and D/ST positions

## Notes

- Long TD bonuses (40+ and 50+ yards) are calculated from play-by-play data
- The tool automatically joins player statistics with long TD bonuses
- D/ST statistics are processed separately from team statistics
- All output files are saved as CSV format for easy import into analysis tools
- Player names are fuzzy-matched, so partial names or variations will work


