"""Data processing and joining logic for fantasy statistics."""

import polars as pl
from typing import Dict, Optional
from rapidfuzz import process
from scoring import (
    porchcrew_offense_points,
    porchcrew_kicker_points,
    porchcrew_dst_points,
)


def calculate_long_td_bonuses(pbp_df: pl.DataFrame) -> Dict[str, pl.DataFrame]:
    """
    Calculate 40+ and 50+ yard TD bonuses for QB, RB, and WR from play-by-play data.

    Args:
        pbp_df: Play-by-play DataFrame.

    Returns:
        Dictionary with keys 'qb', 'rb', 'wr' containing DataFrames with
        long TD bonuses aggregated by (season, week, player_id).
    """
    # Add flags for long TDs
    plays = pbp_df.with_columns([
        # Passing TDs
        (
            (pl.col("pass") == 1)
            & (pl.col("touchdown") == 1)
            & (pl.col("yards_gained") >= 40)
        ).cast(int).alias("ptd40_flag"),
        (
            (pl.col("pass") == 1)
            & (pl.col("touchdown") == 1)
            & (pl.col("yards_gained") >= 50)
        ).cast(int).alias("ptd50_flag"),
        # Rushing TDs
        (
            (pl.col("rush_attempt") == 1)
            & (pl.col("touchdown") == 1)
            & (pl.col("yards_gained") >= 40)
        ).cast(int).alias("rtd40_flag"),
        (
            (pl.col("rush_attempt") == 1)
            & (pl.col("touchdown") == 1)
            & (pl.col("yards_gained") >= 50)
        ).cast(int).alias("rtd50_flag"),
        # Receiving TDs (pass play + receiving TD)
        (
            (pl.col("pass") == 1)
            & (pl.col("pass_touchdown") == 1)
            & (pl.col("receiver_id").is_not_null())
            & (pl.col("yards_gained") >= 40)
        ).cast(int).alias("retd40_flag"),
        (
            (pl.col("pass") == 1)
            & (pl.col("pass_touchdown") == 1)
            & (pl.col("receiver_id").is_not_null())
            & (pl.col("yards_gained") >= 50)
        ).cast(int).alias("retd50_flag"),
    ])

    # Aggregate by players and week
    qb_long_tds = (
        plays
        .filter(pl.col("passer_id").is_not_null())
        .group_by(["season", "week", "passer_id"])
        .agg([
            pl.col("ptd40_flag").sum().alias("pass_td_40p"),
            pl.col("ptd50_flag").sum().alias("pass_td_50p"),
        ])
    )

    rb_long_tds = (
        plays
        .filter(pl.col("rusher_id").is_not_null())
        .group_by(["season", "week", "rusher_id"])
        .agg([
            pl.col("rtd40_flag").sum().alias("rush_td_40p"),
            pl.col("rtd50_flag").sum().alias("rush_td_50p"),
        ])
    )

    wr_long_tds = (
        plays
        .filter(pl.col("receiver_id").is_not_null())
        .group_by(["season", "week", "receiver_id"])
        .agg([
            pl.col("retd40_flag").sum().alias("rec_td_40p"),
            pl.col("retd50_flag").sum().alias("rec_td_50p"),
        ])
    )

    return {
        "qb": qb_long_tds,
        "rb": rb_long_tds,
        "wr": wr_long_tds,
    }


def join_stats_with_long_tds(
    stats_df: pl.DataFrame,
    long_tds: Dict[str, pl.DataFrame],
) -> pl.DataFrame:
    """
    Join player stats with long TD bonuses.

    Args:
        stats_df: Player statistics DataFrame.
        long_tds: Dictionary with 'qb', 'rb', 'wr' DataFrames containing long TD bonuses.

    Returns:
        DataFrame with stats joined with long TD bonuses.
    """
    result = stats_df

    # Join QB long TDs (using passer_id -> player_id mapping)
    if "qb" in long_tds and len(long_tds["qb"]) > 0:
        qb_tds = (
            long_tds["qb"]
            .rename({"passer_id": "player_id"})
            .select(["season", "week", "player_id", "pass_td_40p", "pass_td_50p"])
        )
        # Drop any existing columns that might conflict before joining
        cols_to_drop = [col for col in ["pass_td_40p", "pass_td_50p"] if col in result.columns]
        if cols_to_drop:
            result = result.drop(cols_to_drop)
        result = result.join(
            qb_tds,
            on=["season", "week", "player_id"],
            how="left",
        )
        # Fill nulls with 0
        result = result.with_columns([
            pl.col("pass_td_40p").fill_null(0),
            pl.col("pass_td_50p").fill_null(0),
        ])

    # Join RB long TDs (using rusher_id -> player_id mapping)
    if "rb" in long_tds and len(long_tds["rb"]) > 0:
        rb_tds = (
            long_tds["rb"]
            .rename({"rusher_id": "player_id"})
            .select(["season", "week", "player_id", "rush_td_40p", "rush_td_50p"])
        )
        # Drop any existing columns that might conflict before joining
        cols_to_drop = [col for col in ["rush_td_40p", "rush_td_50p"] if col in result.columns]
        if cols_to_drop:
            result = result.drop(cols_to_drop)
        result = result.join(
            rb_tds,
            on=["season", "week", "player_id"],
            how="left",
        )
        # Fill nulls with 0
        result = result.with_columns([
            pl.col("rush_td_40p").fill_null(0),
            pl.col("rush_td_50p").fill_null(0),
        ])

    # Join WR long TDs (using receiver_id -> player_id mapping)
    if "wr" in long_tds and len(long_tds["wr"]) > 0:
        wr_tds = (
            long_tds["wr"]
            .rename({"receiver_id": "player_id"})
            .select(["season", "week", "player_id", "rec_td_40p", "rec_td_50p"])
        )
        # Drop any existing columns that might conflict before joining
        cols_to_drop = [col for col in ["rec_td_40p", "rec_td_50p"] if col in result.columns]
        if cols_to_drop:
            result = result.drop(cols_to_drop)
        result = result.join(
            wr_tds,
            on=["season", "week", "player_id"],
            how="left",
        )
        # Fill nulls with 0
        result = result.with_columns([
            pl.col("rec_td_40p").fill_null(0),
            pl.col("rec_td_50p").fill_null(0),
        ])

    # Ensure all long TD columns exist (fill with 0 if they don't)
    long_td_cols = [
        "pass_td_40p", "pass_td_50p",
        "rush_td_40p", "rush_td_50p",
        "rec_td_40p", "rec_td_50p",
    ]
    for col in long_td_cols:
        if col not in result.columns:
            result = result.with_columns(pl.lit(0).alias(col))

    return result


def calculate_fantasy_points(
    stats_df: pl.DataFrame,
    position: Optional[str] = None,
) -> pl.DataFrame:
    """
    Calculate fantasy points for players by applying appropriate scoring function.

    Args:
        stats_df: Player statistics DataFrame with all required stat columns.
        position: Optional position filter. If None, applies scoring based on
                  position column in DataFrame.

    Returns:
        DataFrame with added 'fantasy_points' column.
    """
    result = stats_df.clone()

    # Convert DataFrame rows to dicts for scoring functions
    def calculate_row_points(row_dict: Dict) -> float:
        """Calculate points for a single row based on position."""
        pos = row_dict.get("position") or ""
        pos = pos.upper() if pos else ""
        
        if pos in ["QB", "RB", "WR", "TE"]:
            return porchcrew_offense_points(row_dict)
        elif pos == "K":
            return porchcrew_kicker_points(row_dict)
        else:
            # Default to offense scoring for unknown positions
            return porchcrew_offense_points(row_dict)

    # Apply scoring function row by row
    # Convert each row to dict, calculate points, then add as column
    fantasy_points = []
    for row in result.iter_rows(named=True):
        points = calculate_row_points(row)
        fantasy_points.append(points)

    result = result.with_columns(pl.Series("fantasy_points", fantasy_points))

    return result


def calculate_dst_basic_stats(pbp_df: pl.DataFrame) -> pl.DataFrame:
    """
    Calculate basic defensive stats from play-by-play data.

    Args:
        pbp_df: Play-by-play DataFrame.

    Returns:
        DataFrame with basic D/ST stats aggregated by (season, week, defteam).
    """
    # Calculate sacks
    sacks = (
        pbp_df
        .filter(pl.col("sack") == 1)
        .group_by(["season", "week", "defteam"])
        .agg([
            pl.col("sack").sum().alias("sacks"),
        ])
    )

    # Calculate interceptions
    # Interception must occur - the defteam on the play is the team that got the interception
    # We need to count interceptions where defteam got it (not posteam)
    # In nflreadpy: when interception==1, the defteam is typically the team that got it
    # But we should verify by checking that defteam != posteam (defensive team got it, not offensive)
    interceptions = (
        pbp_df
        .filter(
            (pl.col("interception") == 1)
            & (pl.col("defteam") != pl.col("posteam"))  # Ensure defensive team got it, not offensive
        )
        .group_by(["season", "week", "defteam"])
        .agg([
            pl.len().alias("interceptions"),
        ])
    )

    # Calculate safeties
    safeties = (
        pbp_df
        .filter(pl.col("safety") == 1)
        .group_by(["season", "week", "defteam"])
        .agg([
            pl.col("safety").sum().alias("safeties"),
        ])
    )

    # Calculate fumbles recovered
    # Fumble must occur AND the defensive team must recover it
    # Check both fumble_recovery_1_team and fumble_recovery_2_team (some plays have multiple recoveries)
    fumbles_recovered = (
        pbp_df
        .filter(
            (pl.col("fumble") == 1)
            & (
                (pl.col("fumble_recovery_1_team").is_not_null() & (pl.col("fumble_recovery_1_team") == pl.col("defteam")))
                | (pl.col("fumble_recovery_2_team").is_not_null() & (pl.col("fumble_recovery_2_team") == pl.col("defteam")))
            )
        )
        .group_by(["season", "week", "defteam"])
        .agg([
            pl.len().alias("fumbles_recovered"),
        ])
    )

    # Calculate blocked kicks
    # Blocked kicks are credited to the defensive team (defteam)
    blocked_kicks = (
        pbp_df
        .filter(
            (pl.col("punt_blocked") == 1)
            | (pl.col("field_goal_result") == "blocked")
            | (pl.col("extra_point_result") == "blocked")
        )
        .group_by(["season", "week", "defteam"])
        .agg([
            pl.len().alias("blocked_kicks"),
        ])
    )

    # Combine all basic stats using outer joins to handle missing teams
    # Start with a base that includes all teams that have ANY defensive stat
    # Combine all stat DataFrames to get the union of all teams
    all_teams_list = []
    for stat_df in [sacks, interceptions, safeties, fumbles_recovered, blocked_kicks]:
        if len(stat_df) > 0:
            all_teams_list.append(stat_df.select(["season", "week", "defteam"]))
    
    if all_teams_list:
        # Get unique (season, week, defteam) combinations from all stats
        base_teams = pl.concat(all_teams_list).unique(["season", "week", "defteam"])
    else:
        # Fallback: get all teams from pbp_df
        base_teams = pbp_df.select(["season", "week", "defteam"]).unique(["season", "week", "defteam"])
    
    # Start with base teams and add sacks
    if len(sacks) > 0:
        result = base_teams.join(
            sacks.select(["season", "week", "defteam", "sacks"]),
            on=["season", "week", "defteam"],
            how="left",
        )
    else:
        result = base_teams.with_columns([pl.lit(0).alias("sacks")])

    # Join each stat, selecting only the needed columns
    # Use unique suffixes for each join to avoid conflicts
    stat_joins = [
        (interceptions, "interceptions", "_int"),
        (safeties, "safeties", "_saf"),
        (fumbles_recovered, "fumbles_recovered", "_fr"),
        (blocked_kicks, "blocked_kicks", "_blk"),
    ]
    
    for stat_df, col_name, suffix in stat_joins:
        if len(stat_df) > 0:
            # Only select the data column, join keys will come from left side
            stat_df_clean = stat_df.select(["season", "week", "defteam", col_name])
            result = result.join(
                stat_df_clean,
                left_on=["season", "week", "defteam"],
                right_on=["season", "week", "defteam"],
                how="left",
                suffix=suffix,
            )
            # Rename the suffixed data column back to the original name
            suffixed_col = f"{col_name}{suffix}"
            if suffixed_col in result.columns:
                result = result.rename({suffixed_col: col_name})
            # Also handle any suffixed join keys (shouldn't happen but just in case)
            for key in ["season", "week", "defteam"]:
                suffixed_key = f"{key}{suffix}"
                if suffixed_key in result.columns and key in result.columns:
                    # Keep the original, drop the suffixed one
                    result = result.drop(suffixed_key)

    # Fill nulls with 0 and rename defteam to team
    result = (
        result
        .with_columns([
            pl.col("interceptions").fill_null(0),
            pl.col("safeties").fill_null(0),
            pl.col("fumbles_recovered").fill_null(0),
            pl.col("blocked_kicks").fill_null(0),
            pl.col("sacks").fill_null(0),
        ])
        .rename({"defteam": "team"})
    )

    return result


def calculate_dst_touchdowns(pbp_df: pl.DataFrame) -> pl.DataFrame:
    """
    Calculate defensive and special teams touchdowns from play-by-play data.

    Args:
        pbp_df: Play-by-play DataFrame.

    Returns:
        DataFrame with D/ST touchdown stats aggregated by (season, week, defteam).
    """
    # Interception return TDs
    int_td = (
        pbp_df
        .filter(
            (pl.col("interception") == 1)
            & (pl.col("return_touchdown") == 1)
            & (pl.col("td_team") == pl.col("defteam"))
        )
        .group_by(["season", "week", "defteam"])
        .agg([
            pl.len().alias("int_td"),
        ])
    )

    # Fumble return TDs
    fum_ret_td = (
        pbp_df
        .filter(
            (pl.col("fumble") == 1)
            & (pl.col("fumble_lost") == 1)
            & (pl.col("return_touchdown") == 1)
            & (pl.col("td_team") == pl.col("defteam"))
        )
        .group_by(["season", "week", "defteam"])
        .agg([
            pl.len().alias("fum_ret_td"),
        ])
    )

    # Kick return TDs
    kr_td = (
        pbp_df
        .filter(
            (pl.col("kickoff_attempt") == 1)
            & (pl.col("return_touchdown") == 1)
        )
        .group_by(["season", "week", "return_team"])
        .agg([
            pl.len().alias("kr_td"),
        ])
        .rename({"return_team": "defteam"})
    )

    # Punt return TDs
    pr_td = (
        pbp_df
        .filter(
            (pl.col("punt_attempt") == 1)
            & (pl.col("return_touchdown") == 1)
        )
        .group_by(["season", "week", "return_team"])
        .agg([
            pl.len().alias("pr_td"),
        ])
        .rename({"return_team": "defteam"})
    )

    # Blocked kick return TDs
    # Logic: (punt_blocked==1 OR field_goal_result=="blocked" OR extra_point_result=="blocked") & return_touchdown==1
    blk_kick_td = (
        pbp_df
        .filter(
            (pl.col("return_touchdown") == 1)
            & (
                (pl.col("punt_blocked") == 1)
                | (pl.col("field_goal_result") == "blocked")
                | (pl.col("extra_point_result") == "blocked")
            )
        )
        .group_by(["season", "week", "defteam"])
        .agg([
            pl.len().alias("blk_kick_td"),
        ])
    )

    # Two-point conversion returns
    two_pt_returns = (
        pbp_df
        .filter(pl.col("defensive_two_point_conv") == 1)
        .group_by(["season", "week", "defteam"])
        .agg([
            pl.len().alias("two_pt_returns"),
        ])
    )

    # One-point safeties
    one_pt_safeties = (
        pbp_df
        .filter(
            (pl.col("defensive_extra_point_conv") == 1)
            | (pl.col("extra_point_result") == "safety")
        )
        .group_by(["season", "week", "defteam"])
        .agg([
            pl.len().alias("one_pt_safeties"),
        ])
    )

    # Combine all TD stats using outer joins
    # Start with int_td (or create empty structure if no int_tds)
    if len(int_td) > 0:
        result = int_td.select(["season", "week", "defteam", "int_td"])
    else:
        # Create empty structure with required columns
        result = pbp_df.select([
            "season", "week", "defteam"
        ]).unique(["season", "week", "defteam"]).with_columns([
            pl.lit(0).alias("int_td"),
        ])

    # Join each TD stat, selecting only the needed columns
    # Use unique suffixes for each join to avoid conflicts
    td_joins = [
        (fum_ret_td, "fum_ret_td", "_frtd"),
        (kr_td, "kr_td", "_krtd"),
        (pr_td, "pr_td", "_prtd"),
        (blk_kick_td, "blk_kick_td", "_blktd"),
        (two_pt_returns, "two_pt_returns", "_2pt"),
        (one_pt_safeties, "one_pt_safeties", "_1psf"),
    ]
    
    for stat_df, col_name, suffix in td_joins:
        # Always join, even if empty (will create nulls which we fill later)
        # Only select the data column, join keys will come from left side
        stat_df_clean = stat_df.select(["season", "week", "defteam", col_name])
        result = result.join(
            stat_df_clean,
            left_on=["season", "week", "defteam"],
            right_on=["season", "week", "defteam"],
            how="outer",
            suffix=suffix,
        )
        # Rename the suffixed data column back to the original name
        suffixed_col = f"{col_name}{suffix}"
        if suffixed_col in result.columns:
            result = result.rename({suffixed_col: col_name})
        # Clean up any suffixed join keys - these shouldn't exist but polars sometimes creates them
        for key in ["season", "week", "defteam"]:
            suffixed_key = f"{key}{suffix}"
            if suffixed_key in result.columns:
                # If the original key exists, drop the suffixed one
                if key in result.columns:
                    result = result.drop(suffixed_key)
                else:
                    # If original doesn't exist, rename suffixed back to original
                    result = result.rename({suffixed_key: key})

    # Fill nulls with 0 and rename defteam to team
    result = (
        result
        .with_columns([
            pl.col("int_td").fill_null(0),
            pl.col("fum_ret_td").fill_null(0),
            pl.col("kr_td").fill_null(0),
            pl.col("pr_td").fill_null(0),
            pl.col("blk_kick_td").fill_null(0),
            pl.col("two_pt_returns").fill_null(0),
            pl.col("one_pt_safeties").fill_null(0),
        ])
        .rename({"defteam": "team"})
    )

    return result


def calculate_points_allowed(pbp_df: pl.DataFrame) -> pl.DataFrame:
    """
    Calculate points allowed per team per game from play-by-play data.

    Args:
        pbp_df: Play-by-play DataFrame.

    Returns:
        DataFrame with points_allowed aggregated by (season, week, team).
    """
    # Get max scores per game
    game_scores = (
        pbp_df
        .group_by(["game_id", "season", "week", "home_team", "away_team"])
        .agg([
            pl.col("total_home_score").max().alias("final_home_score"),
            pl.col("total_away_score").max().alias("final_away_score"),
        ])
    )

    # Calculate points allowed for home team (away team's score)
    home_pa = (
        game_scores
        .select([
            "season",
            "week",
            pl.col("home_team").alias("team"),
            pl.col("final_away_score").alias("points_allowed"),
        ])
    )

    # Calculate points allowed for away team (home team's score)
    away_pa = (
        game_scores
        .select([
            "season",
            "week",
            pl.col("away_team").alias("team"),
            pl.col("final_home_score").alias("points_allowed"),
        ])
    )

    # Combine home and away
    result = pl.concat([home_pa, away_pa])

    # Fill nulls with 0
    result = result.with_columns([
        pl.col("points_allowed").fill_null(0),
    ])

    return result


def calculate_yards_allowed(pbp_df: pl.DataFrame) -> pl.DataFrame:
    """
    Calculate yards allowed per team from play-by-play data.

    Args:
        pbp_df: Play-by-play DataFrame.

    Returns:
        DataFrame with yards_allowed aggregated by (season, week, defteam).
    """
    # Filter to offensive plays
    offensive_plays = pbp_df.filter(
        pl.col("play_type").is_in(["run", "pass", "qb_kneel", "scramble"])
        | (pl.col("rush") == 1)
        | (pl.col("pass") == 1)
    )

    # Sum yards_gained from defensive perspective (defteam)
    yards_allowed = (
        offensive_plays
        .group_by(["season", "week", "defteam"])
        .agg([
            pl.col("yards_gained").sum().alias("yards_allowed"),
        ])
        .rename({"defteam": "team"})
    )

    # Fill nulls with 0
    yards_allowed = yards_allowed.with_columns([
        pl.col("yards_allowed").fill_null(0),
    ])

    return yards_allowed


def process_dst_stats(pbp_df: pl.DataFrame) -> pl.DataFrame:
    """
    Process and format D/ST stats for scoring using play-by-play data.

    Args:
        pbp_df: Play-by-play DataFrame.

    Returns:
        DataFrame formatted for DST scoring with all required columns and fantasy_points.
    """
    # Calculate all D/ST components
    basic_stats = calculate_dst_basic_stats(pbp_df)
    touchdowns = calculate_dst_touchdowns(pbp_df)
    points_allowed = calculate_points_allowed(pbp_df)
    yards_allowed = calculate_yards_allowed(pbp_df)

    # Start with points_allowed as base (every team should have this)
    # Then join all other stats, selecting only needed columns to avoid conflicts
    dst_df = points_allowed.select(["season", "week", "team", "points_allowed"])
    
    # Join yards_allowed
    if len(yards_allowed) > 0:
        yards_clean = yards_allowed.select(["season", "week", "team", "yards_allowed"])
        dst_df = dst_df.join(yards_clean, on=["season", "week", "team"], how="outer", suffix="_ya")
        # Rename suffixed column back if it exists
        if "yards_allowed_ya" in dst_df.columns:
            dst_df = dst_df.rename({"yards_allowed_ya": "yards_allowed"})
    
    # Join basic_stats (select all stat columns)
    if len(basic_stats) > 0:
        basic_cols = ["season", "week", "team", "sacks", "interceptions", 
                      "safeties", "fumbles_recovered", "blocked_kicks"]
        basic_clean = basic_stats.select([col for col in basic_cols if col in basic_stats.columns])
        dst_df = dst_df.join(basic_clean, on=["season", "week", "team"], how="outer", suffix="_basic")
        # Rename all suffixed columns back
        for col in basic_cols:
            if col not in ["season", "week", "team"]:
                suffixed_col = f"{col}_basic"
                if suffixed_col in dst_df.columns:
                    dst_df = dst_df.rename({suffixed_col: col})
    
    # Join touchdowns (select all TD columns)
    if len(touchdowns) > 0:
        td_cols = ["season", "week", "team", "int_td", "fum_ret_td", "kr_td", 
                   "pr_td", "blk_kick_td", "two_pt_returns", "one_pt_safeties"]
        td_clean = touchdowns.select([col for col in td_cols if col in touchdowns.columns])
        dst_df = dst_df.join(td_clean, on=["season", "week", "team"], how="outer", suffix="_td")
        # Rename all suffixed columns back
        for col in td_cols:
            if col not in ["season", "week", "team"]:
                suffixed_col = f"{col}_td"
                if suffixed_col in dst_df.columns:
                    dst_df = dst_df.rename({suffixed_col: col})

    # Ensure all required columns exist (fill with 0 if missing)
    required_cols = {
        "sacks": 0,
        "interceptions": 0,
        "safeties": 0,
        "fumbles_recovered": 0,
        "blocked_kicks": 0,
        "int_td": 0,
        "fum_ret_td": 0,
        "kr_td": 0,
        "pr_td": 0,
        "blk_kick_td": 0,
        "two_pt_returns": 0,
        "one_pt_safeties": 0,
        "points_allowed": 0,
        "yards_allowed": 0,
    }

    for col, default_value in required_cols.items():
        if col not in dst_df.columns:
            dst_df = dst_df.with_columns(pl.lit(default_value).alias(col))
        else:
            dst_df = dst_df.with_columns(pl.col(col).fill_null(default_value))

    # Calculate fantasy points for DST
    fantasy_points = []
    for row in dst_df.iter_rows(named=True):
        points = porchcrew_dst_points(row)
        fantasy_points.append(points)

    dst_df = dst_df.with_columns(pl.Series("fantasy_points", fantasy_points))

    return dst_df


def find_player_by_name(stats_df: pl.DataFrame, player_name: str, score_cutoff: int = 60) -> Optional[str]:
    """
    Find the best matching player display name using fuzzy string matching.

    Args:
        stats_df: Player statistics DataFrame with 'player_display_name' column.
        player_name: Name to search for (can be partial or misspelled).
        score_cutoff: Minimum similarity score (0-100) to consider a match. Default 60.

    Returns:
        The matched player_display_name if a good match is found, None otherwise.
    """
    if "player_display_name" not in stats_df.columns:
        return None

    # Get unique player display names from the DataFrame
    unique_names = stats_df.select("player_display_name").unique().to_series().to_list()

    if not unique_names:
        return None

    # Use rapidfuzz to find the best match
    result = process.extractOne(
        player_name,
        unique_names,
        score_cutoff=score_cutoff
    )

    if result is None:
        return None

    matched_name, score, _ = result
    return matched_name


def find_team_by_name(dst_df: pl.DataFrame, team_name: str, score_cutoff: int = 60) -> Optional[str]:
    """
    Find the best matching team abbreviation using fuzzy string matching.

    Args:
        dst_df: D/ST statistics DataFrame with 'team' column.
        team_name: Team name or abbreviation to search for (e.g., "Buffalo", "BUF", "Bills").
        score_cutoff: Minimum similarity score (0-100) to consider a match. Default 60.

    Returns:
        The matched team abbreviation if a good match is found, None otherwise.
    """
    if "team" not in dst_df.columns:
        return None

    # Get unique team abbreviations from the DataFrame
    unique_teams = dst_df.select("team").unique().to_series().to_list()

    if not unique_teams:
        return None

    # Use rapidfuzz to find the best match
    result = process.extractOne(
        team_name,
        unique_teams,
        score_cutoff=score_cutoff
    )

    if result is None:
        return None

    matched_team, score, _ = result
    return matched_team

