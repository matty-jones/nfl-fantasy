"""Functions to load data from nflreadpy."""

import nflreadpy as nfl
import polars as pl
from typing import List


def load_play_by_play(seasons: List[int]) -> pl.DataFrame:
    """
    Load and filter play-by-play data for specified seasons.

    Args:
        seasons: List of seasons to load (e.g., [2025]).

    Returns:
        DataFrame containing play-by-play data filtered to specified seasons.
    """
    pbp = nfl.load_pbp(seasons=seasons)
    return pbp.filter(pl.col("season").is_in(seasons))


def load_player_stats(seasons: List[int]) -> pl.DataFrame:
    """
    Load player statistics for specified seasons.

    Args:
        seasons: List of seasons to load (e.g., [2025]).

    Returns:
        DataFrame containing weekly player statistics.
    """
    return nfl.load_player_stats(seasons, summary_level="week")

