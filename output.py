"""Output generation functions for fantasy statistics."""

import polars as pl
from pathlib import Path
from typing import Dict, Optional, List


def generate_player_outputs(
    stats_df: pl.DataFrame,
    week_list: Optional[List[int]] = None,
) -> Dict[str, pl.DataFrame]:
    """
    Generate separate output tables by position (QB, RB, WR/TE, K).

    Args:
        stats_df: Player statistics DataFrame with fantasy_points column.
        week_list: Optional list of weeks to filter to.

    Returns:
        Dictionary with keys 'qb', 'rb', 'wr_te', 'k' containing filtered DataFrames.
    """
    # Filter by week if specified
    if week_list is not None:
        stats_df = stats_df.filter(pl.col("week").is_in(week_list))

    # Select relevant columns for output
    # Include week, team, player name, all raw stats, and fantasy score
    output_cols = [
        "season",
        "week",
        "player_id",
        "player_name",
        "team",
        "position",
    ]

    # Add all stat columns (exclude metadata columns)
    stat_cols = [
        col for col in stats_df.columns
        if col not in output_cols and col != "fantasy_points"
    ]
    output_cols.extend(stat_cols)
    output_cols.append("fantasy_points")

    # Filter to only columns that exist
    available_cols = [col for col in output_cols if col in stats_df.columns]
    base_output = stats_df.select(available_cols)

    # Generate position-specific outputs
    outputs = {}

    # QB
    qb_df = base_output.filter(pl.col("position") == "QB")
    if len(qb_df) > 0:
        outputs["qb"] = qb_df

    # RB
    rb_df = base_output.filter(pl.col("position") == "RB")
    if len(rb_df) > 0:
        outputs["rb"] = rb_df

    # WR/TE (combined)
    wr_te_df = base_output.filter(
        pl.col("position").is_in(["WR", "TE"])
    )
    if len(wr_te_df) > 0:
        outputs["wr_te"] = wr_te_df

    # K
    k_df = base_output.filter(pl.col("position") == "K")
    if len(k_df) > 0:
        outputs["k"] = k_df

    return outputs


def generate_dst_output(
    dst_df: pl.DataFrame,
    week_list: Optional[List[int]] = None,
) -> pl.DataFrame:
    """
    Generate D/ST output table.

    Args:
        dst_df: DST statistics DataFrame with fantasy_points column.
        week_list: Optional list of weeks to filter to.

    Returns:
        DataFrame with DST statistics including fantasy_points.
    """
    if week_list is not None:
        dst_df = dst_df.filter(pl.col("week").is_in(week_list))

    # Ensure all relevant columns are included
    # The DST DataFrame should already have season, week, team, stats, and fantasy_points
    return dst_df


def save_outputs(
    outputs: Dict[str, pl.DataFrame],
    dst_df: Optional[pl.DataFrame] = None,
    output_dir: str = "output",
    week_list: Optional[List[int]] = None,
) -> None:
    """
    Save output tables as CSV files.

    Args:
        outputs: Dictionary of position-specific DataFrames (from generate_player_outputs).
        dst_df: Optional DST DataFrame.
        output_dir: Directory to save output files.
        week_list: Optional list of weeks for filename suffix.
    """
    output_path = Path(output_dir)
    output_path.mkdir(parents=True, exist_ok=True)

    # Format week suffix for filename
    week_suffix = ""
    if week_list is not None:
        if len(week_list) == 1:
            week_suffix = f"_week_{week_list[0]}"
        elif len(week_list) > 1:
            # Format as range if consecutive, otherwise comma-separated
            if len(week_list) == week_list[-1] - week_list[0] + 1 and week_list == list(range(week_list[0], week_list[-1] + 1)):
                week_suffix = f"_week_{week_list[0]}-{week_list[-1]}"
            else:
                week_suffix = f"_week_{','.join(map(str, week_list))}"

    # Save player position outputs
    for position, df in outputs.items():
        filename = f"{position}_stats{week_suffix}.csv"
        filepath = output_path / filename
        df.write_csv(filepath)
        print(f"Saved {filepath}")

    # Save DST output
    if dst_df is not None and len(dst_df) > 0:
        filename = f"dst_stats{week_suffix}.csv"
        filepath = output_path / filename
        dst_df.write_csv(filepath)
        print(f"Saved {filepath}")

