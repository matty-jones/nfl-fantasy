"""Summary statistics calculation for fantasy players and teams."""

import polars as pl
from typing import Optional, List, Dict, Any


def _calculate_stats_from_df(df: pl.DataFrame) -> Dict[str, Any]:
    """
    Calculate summary statistics from a DataFrame (helper function).
    
    Args:
        df: DataFrame with fantasy_points column
        
    Returns:
        Dictionary with statistics
    """
    if len(df) == 0 or "fantasy_points" not in df.columns:
        return {
            "num_games": 0,
            "mean_points": 0.0,
            "median_points": 0.0,
            "max_points": 0.0,
            "min_points": 0.0,
            "stddev_points": 0.0,
            "mad_points": 0.0,
            "nuclear_games": 0,
            "boom_games": 0,
            "bust_games": 0,
        }
    
    points = df["fantasy_points"]
    
    num_games = len(df)
    mean_points = float(points.mean())
    median_points = float(points.median())
    max_points = float(points.max())
    min_points = float(points.min())
    
    # Calculate standard deviation
    # std() returns None when there's only 1 data point (undefined)
    std_result = points.std()
    stddev_points = float(std_result) if std_result is not None else 0.0
    
    # Calculate Median Absolute Deviation (MAD)
    # MAD = median(|x_i - median(x)|)
    if num_games > 0:
        deviations = (points - median_points).abs()
        mad_points = float(deviations.median())
    else:
        mad_points = 0.0
    
    nuclear_games = int((points >= 20).sum())
    boom_games = int((points >= 15).sum())
    bust_games = int((points < 10).sum())
    
    return {
        "num_games": num_games,
        "mean_points": mean_points,
        "median_points": median_points,
        "max_points": max_points,
        "min_points": min_points,
        "stddev_points": stddev_points,
        "mad_points": mad_points,
        "nuclear_games": nuclear_games,
        "boom_games": boom_games,
        "bust_games": bust_games,
    }


def calculate_summary_stats(
    df: pl.DataFrame,
    identifier_col: str,
    identifier_value: str,
    week_list: Optional[List[int]] = None,
) -> Dict[str, Any]:
    """
    Calculate summary statistics for a player or team.
    
    Args:
        df: DataFrame with fantasy_points column and week column
        identifier_col: Column name to filter by (e.g., "player_display_name" or "team")
        identifier_value: Value to filter for
        week_list: Optional list of weeks to filter to. If None, uses all weeks.
        
    Returns:
        Dictionary with statistics:
        - num_games: Count of games
        - mean_points: Mean fantasy points
        - median_points: Median fantasy points
        - max_points: Maximum fantasy points
        - min_points: Minimum fantasy points
        - stddev_points: Standard deviation of fantasy points
        - mad_points: Median Absolute Deviation of fantasy points
        - nuclear_games: Count of games with 20+ points
        - boom_games: Count of games with 15+ points
        - bust_games: Count of games with <10 points
    """
    # Filter to the specific player/team
    filtered_df = df.filter(pl.col(identifier_col) == identifier_value)
    
    # Apply week filter if specified
    if week_list is not None:
        filtered_df = filtered_df.filter(pl.col("week").is_in(week_list))
    
    return _calculate_stats_from_df(filtered_df)


def generate_summary_output(
    player_df: pl.DataFrame,
    dst_df: pl.DataFrame,
    player_names: List[str],
    week_list: Optional[List[int]] = None,
) -> pl.DataFrame:
    """
    Generate summary statistics DataFrame for multiple players/teams.
    
    Calculates statistics for:
    - Season-to-date (all games in season)
    - Recent form (most recent 4 games)
    - Specified weeks (if week_list is provided)
    
    Args:
        player_df: DataFrame with player statistics (must have player_display_name, week, fantasy_points)
        dst_df: DataFrame with D/ST statistics (must have team, week, fantasy_points)
        player_names: List of player display names or team names to summarize
        week_list: Optional list of weeks to calculate statistics for
        
    Returns:
        DataFrame with one row per player/team and columns for each stat/time period
    """
    results = []
    
    for name in player_names:
        # Try to find as a player first
        player_matches = player_df.filter(pl.col("player_display_name") == name)
        is_dst = False
        
        if len(player_matches) == 0:
            # Try as D/ST team
            dst_matches = dst_df.filter(pl.col("team") == name)
            if len(dst_matches) > 0:
                is_dst = True
                df_to_use = dst_df
                identifier_col = "team"
            else:
                # Skip if not found
                continue
        else:
            df_to_use = player_df
            identifier_col = "player_display_name"
        
        # Calculate season-to-date stats (all games)
        season_stats = calculate_summary_stats(
            df_to_use, identifier_col, name, week_list=None
        )
        
        # Calculate recent form (most recent 4 games)
        # Get all games for this player/team, sorted by week descending
        all_games = df_to_use.filter(pl.col(identifier_col) == name)
        if len(all_games) > 0:
            # Sort by week descending and take the 4 most recent games
            recent_games = all_games.sort("week", descending=True).head(4)
            # Calculate stats directly on these recent games
            recent_stats = _calculate_stats_from_df(recent_games)
        else:
            recent_stats = {
                "num_games": 0,
                "mean_points": 0.0,
                "median_points": 0.0,
                "max_points": 0.0,
                "min_points": 0.0,
                "stddev_points": 0.0,
                "mad_points": 0.0,
                "nuclear_games": 0,
                "boom_games": 0,
                "bust_games": 0,
            }
        
        # Calculate stats for specified weeks (if provided)
        if week_list is not None:
            weeks_stats = calculate_summary_stats(
                df_to_use, identifier_col, name, week_list=week_list
            )
        else:
            weeks_stats = {
                "num_games": 0,
                "mean_points": 0.0,
                "median_points": 0.0,
                "max_points": 0.0,
                "min_points": 0.0,
                "stddev_points": 0.0,
                "mad_points": 0.0,
                "nuclear_games": 0,
                "boom_games": 0,
                "bust_games": 0,
            }
        
        # Build result row
        result_row = {
            "name": name,
            "type": "D/ST" if is_dst else "Player",
        }
        
        # Add season-to-date stats with prefix
        for key, value in season_stats.items():
            result_row[f"season_{key}"] = value
        
        # Add recent form stats with prefix
        for key, value in recent_stats.items():
            result_row[f"recent_{key}"] = value
        
        # Add specified weeks stats with prefix (if weeks were specified)
        if week_list is not None:
            for key, value in weeks_stats.items():
                result_row[f"weeks_{key}"] = value
        
        results.append(result_row)
    
    if not results:
        # Return empty DataFrame with expected columns
        columns = ["name", "type"]
        stat_keys = ["num_games", "mean_points", "median_points", "max_points", "min_points",
                     "stddev_points", "mad_points", "nuclear_games", "boom_games", "bust_games"]
        for prefix in ["season_", "recent_"]:
            columns.extend([f"{prefix}{key}" for key in stat_keys])
        if week_list is not None:
            columns.extend([f"weeks_{key}" for key in stat_keys])
        return pl.DataFrame(schema={col: pl.Float64 if "points" in col or "games" in col else pl.Utf8 for col in columns})
    
    return pl.DataFrame(results)

