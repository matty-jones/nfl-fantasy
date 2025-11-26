"""Main entry point for NFL fantasy league data processing."""

import typer
import polars as pl
from typing import Optional, List
from pathlib import Path

from config import DEFAULT_SEASONS, OUTPUT_DIR
from data_loader import load_play_by_play, load_player_stats
from data_processor import (
    calculate_long_td_bonuses,
    join_stats_with_long_tds,
    calculate_fantasy_points,
    process_dst_stats,
    find_player_by_name,
    find_team_by_name,
)
from output import generate_player_outputs, generate_dst_output, save_outputs
from summary import generate_summary_output

app = typer.Typer(help="NFL Fantasy League Data Processor")


def parse_week_spec(week_spec: Optional[str]) -> Optional[List[int]]:
    """
    Parse week specification into a list of week numbers.
    
    Supports:
    - Single week: "11" -> [11]
    - Comma-separated: "8,9,10" -> [8, 9, 10]
    - Range: "8-10" -> [8, 9, 10]
    - Mixed: "8,9,11-13" -> [8, 9, 11, 12, 13]
    
    Args:
        week_spec: Week specification string or None
        
    Returns:
        List of week numbers or None if week_spec is None
    """
    if week_spec is None:
        return None
    
    week_spec = week_spec.strip()
    if not week_spec:
        return None
    
    weeks = set()
    
    # Split by comma first
    parts = [p.strip() for p in week_spec.split(",")]
    
    for part in parts:
        if not part:
            continue
        
        # Check if it's a range (contains dash)
        if "-" in part:
            range_parts = part.split("-", 1)
            if len(range_parts) == 2:
                try:
                    start = int(range_parts[0].strip())
                    end = int(range_parts[1].strip())
                    # Add all weeks in range (inclusive)
                    weeks.update(range(start, end + 1))
                except ValueError:
                    raise typer.BadParameter(f"Invalid week range: {part}")
            else:
                raise typer.BadParameter(f"Invalid week range format: {part}")
        else:
            # Single week number
            try:
                weeks.add(int(part))
            except ValueError:
                raise typer.BadParameter(f"Invalid week number: {part}")
    
    if not weeks:
        return None
    
    return sorted(list(weeks))


def parse_player_list(player_spec: Optional[str]) -> List[str]:
    """
    Parse comma-separated player names into a list.
    
    Args:
        player_spec: Comma-separated player names or None
        
    Returns:
        List of player name strings (trimmed)
    """
    if player_spec is None:
        return []
    
    players = [p.strip() for p in player_spec.split(",") if p.strip()]
    return players


def parse_team_list(team_spec: Optional[str]) -> List[str]:
    """
    Parse comma-separated team names into a list.
    
    Args:
        team_spec: Comma-separated team names or None
        
    Returns:
        List of team name strings (trimmed)
    """
    if team_spec is None:
        return []
    
    teams = [t.strip() for t in team_spec.split(",") if t.strip()]
    return teams


@app.callback(invoke_without_command=True)
def main(
    seasons: Optional[str] = typer.Option(
        None,
        "--seasons",
        "-s",
        help="Comma-separated list of seasons (e.g., '2025' or '2024,2025')",
    ),
    week: Optional[str] = typer.Option(
        None,
        "--week",
        "-w",
        help="Filter to specific week(s). Supports single week (11), comma-separated (8,9,10), or range (8-10).",
    ),
    output_dir: str = typer.Option(
        OUTPUT_DIR,
        "--output-dir",
        "-o",
        help="Output directory for CSV files",
    ),
    player: Optional[str] = typer.Option(
        None,
        "--player",
        "-p",
        help="Player name to search for (fuzzy-matched). When provided, also generates a player-specific CSV file.",
    ),
    team: Optional[str] = typer.Option(
        None,
        "--team",
        "-t",
        help="D/ST team name to search for (fuzzy-matched). When provided, also generates a team-specific CSV file.",
    ),
    display: bool = typer.Option(
        False,
        "--display",
        "-d",
        help="Display player/team statistics in console (only used with --player or --team)",
    ),
    summary: bool = typer.Option(
        False,
        "--summary",
        help="Generate summary statistics for specified players/teams. Requires --player or --team flag.",
    ),
):
    """
    Process NFL fantasy league data and generate statistics files.

    This command loads player and team statistics, calculates fantasy points
    based on Porch Crew scoring rules, and outputs separate CSV files for
    each position (QB, RB, WR/TE, K, D/ST).

    Use --player to also generate statistics for a specific player (fuzzy-matched by name).
    Use --team to also generate statistics for a specific D/ST team (fuzzy-matched by name).
    """
    # Parse seasons
    if seasons:
        season_list = [int(s.strip()) for s in seasons.split(",")]
    else:
        season_list = DEFAULT_SEASONS

    # Parse week specification
    week_list = parse_week_spec(week)

    typer.echo(f"Processing seasons: {season_list}")
    if week_list:
        typer.echo(f"Filtering to weeks: {week_list}")
    
    # Check if summary is requested without player or team
    if summary and not player and not team:
        typer.echo("Error: --summary requires --player or --team flag to specify players/teams to summarize", err=True)
        raise typer.Exit(1)

    # Load data
    typer.echo("Loading play-by-play data...")
    pbp_df = load_play_by_play(season_list)

    typer.echo("Loading player statistics...")
    player_stats_df = load_player_stats(season_list)

    # Calculate long TD bonuses
    typer.echo("Calculating long TD bonuses...")
    long_tds = calculate_long_td_bonuses(pbp_df)

    # Join stats with long TDs
    typer.echo("Joining stats with long TD bonuses...")
    all_stats_df = join_stats_with_long_tds(player_stats_df, long_tds)

    # Calculate fantasy points
    typer.echo("Calculating fantasy points...")
    stats_with_points_df = calculate_fantasy_points(all_stats_df)

    # Process DST stats from play-by-play data
    typer.echo("Processing D/ST statistics...")
    dst_df = process_dst_stats(pbp_df)

    # Generate outputs
    typer.echo("Generating output tables...")
    player_outputs = generate_player_outputs(stats_with_points_df, week_list=week_list)
    dst_output = generate_dst_output(dst_df, week_list=week_list)

    # Save outputs (only if not in summary mode, or if summary mode but also want regular outputs)
    if not summary:
        typer.echo(f"Saving outputs to {output_dir}...")
        save_outputs(
            player_outputs,
            dst_df=dst_output,
            output_dir=output_dir,
            week_list=week_list,
        )

    # Handle summary mode
    if summary and (player or team):
        typer.echo("\nGenerating summary statistics...")
        
        # Parse player and team lists
        player_names = parse_player_list(player)
        team_names = parse_team_list(team)
        matched_names = []
        
        # Find matches for each player
        for player_name in player_names:
            matched_player = find_player_by_name(stats_with_points_df, player_name)
            if matched_player is not None:
                matched_names.append(matched_player)
            else:
                typer.echo(f"Warning: No player found matching '{player_name}', skipping", err=True)
        
        # Find matches for each team
        for team_name in team_names:
            matched_team = find_team_by_name(dst_df, team_name)
            if matched_team is not None:
                matched_names.append(matched_team)
            else:
                typer.echo(f"Warning: No team found matching '{team_name}', skipping", err=True)
        
        if not matched_names:
            typer.echo("Error: No valid players or teams found", err=True)
            raise typer.Exit(1)
        
        # Generate summary
        summary_df = generate_summary_output(
            stats_with_points_df,
            dst_df,
            matched_names,
            week_list=week_list,
        )
        
        # Display summary
        typer.echo("\n" + "=" * 80)
        typer.echo("Summary Statistics")
        typer.echo("=" * 80)
        print(summary_df)
        typer.echo("=" * 80 + "\n")
        
        # Save summary to CSV
        output_path = Path(output_dir)
        output_path.mkdir(parents=True, exist_ok=True)
        
        filename = "summary_stats"
        if week_list is not None:
            if len(week_list) == 1:
                filename += f"_week_{week_list[0]}"
            elif len(week_list) > 1:
                if len(week_list) == week_list[-1] - week_list[0] + 1 and week_list == list(range(week_list[0], week_list[-1] + 1)):
                    filename += f"_week_{week_list[0]}-{week_list[-1]}"
                else:
                    filename += f"_week_{','.join(map(str, week_list))}"
        filename += ".csv"
        
        filepath = output_path / filename
        summary_df.write_csv(filepath)
        typer.echo(f"Saved summary statistics to {filepath}")
    
    # Handle player/team lookup if requested (non-summary mode)
    elif player or team:
        if player:
            typer.echo(f"\nSearching for player(s): {player}")
        if team:
            typer.echo(f"\nSearching for team(s): {team}")
        
        # Parse player and team lists
        player_names = parse_player_list(player)
        team_names = parse_team_list(team)
        
        if not player_names and not team_names:
            typer.echo("Error: No valid player or team names provided", err=True)
            raise typer.Exit(1)
        
        # Find matches for each player
        matched_players = []
        all_player_dfs = []
        
        for player_name in player_names:
            matched_player = find_player_by_name(stats_with_points_df, player_name)
            
            if matched_player is not None:
                matched_players.append(matched_player)
                typer.echo(f"Found player: {matched_player}")
                
                # Filter to matched player by display name
                player_df = stats_with_points_df.filter(pl.col("player_display_name") == matched_player)
                
                # Apply week filter if specified
                if week_list is not None:
                    player_df = player_df.filter(pl.col("week").is_in(week_list))
                
                if len(player_df) > 0:
                    all_player_dfs.append(player_df)
                else:
                    typer.echo(f"Warning: No data found for {matched_player}", err=True)
            else:
                typer.echo(f"Warning: No player found matching '{player_name}', skipping", err=True)
        
        # Find matches for each team
        matched_teams = []
        all_team_dfs = []
        
        for team_name in team_names:
            matched_team = find_team_by_name(dst_df, team_name)
            if matched_team is not None:
                matched_teams.append(matched_team)
                typer.echo(f"Found D/ST team: {matched_team}")
                
                # Filter to matched team
                team_df = dst_df.filter(pl.col("team") == matched_team)
                
                # Apply week filter if specified
                if week_list is not None:
                    team_df = team_df.filter(pl.col("week").is_in(week_list))
                
                if len(team_df) > 0:
                    all_team_dfs.append(team_df)
                else:
                    typer.echo(f"Warning: No data found for {matched_team} D/ST", err=True)
            else:
                typer.echo(f"Warning: No team found matching '{team_name}', skipping", err=True)
        
        if not all_player_dfs and not all_team_dfs:
            typer.echo("Error: No valid data found for any of the specified players or teams", err=True)
            raise typer.Exit(1)
        
        # Combine all player dataframes
        if all_player_dfs:
            combined_player_df = pl.concat(all_player_dfs)
        else:
            combined_player_df = None
        
        # Combine all team dataframes
        if all_team_dfs:
            combined_team_df = pl.concat(all_team_dfs)
        else:
            combined_team_df = None
        
        # Combine players and teams into a single output
        if combined_player_df is not None and combined_team_df is not None:
            # Both players and teams - need to align columns
            # Use player dataframe column order as base (has more columns)
            player_cols = combined_player_df.columns
            team_cols = set(combined_team_df.columns)
            player_schema = combined_player_df.schema
            team_schema = combined_team_df.schema

            # Add missing columns to team dataframe (fill with None and match dtypes)
            for col in player_cols:
                if col not in team_cols:
                    dtype = player_schema.get(col)
                    if dtype is None:
                        combined_team_df = combined_team_df.with_columns(pl.lit(None).alias(col))
                    else:
                        combined_team_df = combined_team_df.with_columns(pl.lit(None, dtype=dtype).alias(col))

            # Add any team-only columns to player dataframe
            for col in team_cols:
                if col not in player_cols:
                    dtype = team_schema.get(col)
                    if dtype is None:
                        combined_player_df = combined_player_df.with_columns(pl.lit(None).alias(col))
                    else:
                        combined_player_df = combined_player_df.with_columns(pl.lit(None, dtype=dtype).alias(col))

            # Ensure same column order (use player column order)
            all_cols = list(player_cols) + [col for col in team_cols if col not in player_cols]
            combined_player_df = combined_player_df.select(all_cols)
            combined_team_df = combined_team_df.select(all_cols)

            combined_player_df = combined_player_df.sort(["player_id", "season", "week"])
            combined_team_df = combined_team_df.sort(["team", "season", "week"])

            output_df = pl.concat([combined_player_df, combined_team_df])
        elif combined_player_df is not None:
            output_df = combined_player_df.sort(["season", "week"])
        else:
            output_df = combined_team_df.sort(["season", "week"])
        
        # Prepare output columns
        base_cols = ["season", "week"]
        if "player_display_name" in output_df.columns:
            base_cols.extend(["player_id", "player_name", "player_display_name", "team", "position"])
        elif "team" in output_df.columns:
            base_cols.append("team")
        
        # Add all stat columns (exclude metadata columns)
        stat_cols = [
            col for col in output_df.columns
            if col not in base_cols and col != "fantasy_points"
        ]
        output_cols = base_cols + stat_cols + ["fantasy_points"]
        
        # Filter to only columns that exist
        available_cols = [col for col in output_cols if col in output_df.columns]
        output_df = output_df.select(available_cols)

        # Display in console if requested
        if display:
            typer.echo("\n" + "=" * 80)
            if matched_players and matched_teams:
                typer.echo(f"Statistics for {len(matched_players)} player(s) and {len(matched_teams)} team(s)")
            elif matched_players:
                typer.echo(f"Statistics for {len(matched_players)} player(s)")
            else:
                typer.echo(f"Statistics for {len(matched_teams)} team(s)")
            typer.echo("=" * 80)
            print(output_df)
            typer.echo("=" * 80 + "\n")
        
        # Save to CSV
        output_path = Path(output_dir)
        output_path.mkdir(parents=True, exist_ok=True)
        
        # Generate filename based on number of players/teams
        total_items = len(matched_players) + len(matched_teams)
        if total_items == 1:
            # Single player/team - use their name
            if matched_players:
                safe_name = "".join(c if c.isalnum() or c in (" ", "-", "_") else "_" for c in matched_players[0])
                safe_name = safe_name.replace(" ", "_")
                filename = f"{safe_name}_stats"
            else:
                safe_name = "".join(c if c.isalnum() or c in (" ", "-", "_") else "_" for c in matched_teams[0])
                safe_name = safe_name.replace(" ", "_")
                filename = f"{safe_name}_dst_stats"
        else:
            # Multiple players/teams - use generic name
            filename = "combined_stats"
        
        if week_list is not None:
            if len(week_list) == 1:
                filename += f"_week_{week_list[0]}"
            elif len(week_list) > 1:
                if len(week_list) == week_list[-1] - week_list[0] + 1 and week_list == list(range(week_list[0], week_list[-1] + 1)):
                    filename += f"_week_{week_list[0]}-{week_list[-1]}"
                else:
                    filename += f"_week_{','.join(map(str, week_list))}"
        filename += ".csv"
        
        filepath = output_path / filename
        output_df.write_csv(filepath)
        
        if len(player_names) == 1:
            if matched_players:
                typer.echo(f"Saved player statistics to {filepath}")
            else:
                typer.echo(f"Saved D/ST statistics to {filepath}")
        else:
            typer.echo(f"Saved combined statistics to {filepath}")

    typer.echo("Processing complete!")


if __name__ == "__main__":
    app()

