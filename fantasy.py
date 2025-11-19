import nflreadpy as nfl
import polars as pl


def porchcrew_offense_points(row):
    """
    row: a dict-like (e.g. pandas Series) with nflverse offensive columns.
    Returns fantasy points for your Porch Crew scoring, ignoring
    long-TD bonuses unless you provide the *_td_40p / *_td_50p fields.
    """

    pts = 0.0

    # --- Passing ---
    pts += 0.04 * row.get("passing_yards", 0)
    pts += 6.0  * row.get("passing_tds", 0)
    pts += -3.0 * row.get("passing_interceptions", 0)
    pts += 2.0  * row.get("passing_2pt_conversions", 0)

    # Optional: long passing TD bonuses (if you precompute these)
    pts += 2.0 * row.get("pass_td_40p", 0)
    pts += 3.0 * row.get("pass_td_50p", 0)

    # --- Rushing ---
    pts += 0.10 * row.get("rushing_yards", 0)
    pts += 6.0  * row.get("rushing_tds", 0)
    pts += 2.0  * row.get("rushing_2pt_conversions", 0)

    pts += 2.0 * row.get("rush_td_40p", 0)  # optional
    pts += 3.0 * row.get("rush_td_50p", 0)  # optional

    # --- Receiving ---
    pts += 0.10 * row.get("receiving_yards", 0)
    pts += 1.0  * row.get("receptions", 0)
    pts += 6.0  * row.get("receiving_tds", 0)
    pts += 2.0  * row.get("receiving_2pt_conversions", 0)

    pts += 2.0 * row.get("rec_td_40p", 0)   # optional
    pts += 3.0 * row.get("rec_td_50p", 0)   # optional

    # --- Misc / Returns / Defence credited to the player ---
    # Kick/punt return TDs:
    pts += 6.0 * row.get("special_teams_tds", 0)

    # Defensive TDs & fumble-recovery TDs (covers INTTD, FRTD, FTD, BLKKRTD)
    # NB: small risk of double-counting depending on how you build the CSV;
    # you can drop one of these terms if needed.
    pts += 6.0 * row.get("def_tds", 0)
    pts += 6.0 * row.get("fumble_recovery_tds", 0)

    # 1-pt safeties (vanishingly rare, but hey)
    pts += 1.0 * row.get("def_safeties", 0)

    # Total fumbles lost: rush + rec + sack fumbles
    fumbles_lost = (
        row.get("rushing_fumbles_lost", 0)
        + row.get("receiving_fumbles_lost", 0)
        + row.get("sack_fumbles_lost", 0)
    )
    pts += -2.0 * fumbles_lost

    return pts


def porchcrew_kicker_points(row):
    """
    row: dict-like with nflverse kicking columns
         (from stat_type='kicking').
    """

    pts = 0.0

    # Extra points
    pts += 1.0 * row.get("pat_made", 0)

    # Made FGs
    fg_made_0_39 = (
        row.get("fg_made_0_19", 0)
        + row.get("fg_made_20_29", 0)
        + row.get("fg_made_30_39", 0)
    )
    pts += 3.0 * fg_made_0_39
    pts += 4.0 * row.get("fg_made_40_49", 0)
    pts += 5.0 * row.get("fg_made_50_59", 0)
    pts += 6.0 * row.get("fg_made_60_", 0)

    # Missed FGs
    miss_0_19  = row.get("fg_missed_0_19", 0)
    miss_20_29 = row.get("fg_missed_20_29", 0)
    miss_30_39 = row.get("fg_missed_30_39", 0)
    miss_40_49 = row.get("fg_missed_40_49", 0)
    miss_50_59 = row.get("fg_missed_50_59", 0)
    miss_60    = row.get("fg_missed_60_", 0)

    total_missed = row.get(
        "fg_missed",
        miss_0_19 + miss_20_29 + miss_30_39 + miss_40_49 + miss_50_59 + miss_60,
    )

    # -1 for every miss
    pts += -1.0 * total_missed

    # Extra penalties per your settings
    miss_0_39_total = miss_0_19 + miss_20_29 + miss_30_39
    pts += -3.0 * miss_0_39_total    # -3 more → net -4 each
    pts += -2.0 * miss_40_49         # -2 more → net -3 each
    # 50+ only get the -1 generic

    return pts



def dst_points_allowed_component(points_allowed: int) -> int:
    """ESPN LM PA* buckets."""
    pa = points_allowed

    if pa == 0:                      # PA0
        return 8
    elif 1 <= pa <= 6:               # PA1
        return 4
    elif 7 <= pa <= 13:              # PA7
        return 3
    elif 14 <= pa <= 17:             # PA14
        return 1
    elif 18 <= pa <= 27:             # (no PA code; 0 pts)
        return 0
    elif 28 <= pa <= 34:             # PA28
        return -1
    elif 35 <= pa <= 45:             # PA35
        return -3
    else:                            # pa >= 46  -> PA46
        return -5


def dst_yards_allowed_component(yards_allowed: int) -> int:
    """ESPN LM YA* buckets."""
    ya = yards_allowed

    if ya < 100:                     # YA100
        return 8
    elif ya < 200:                   # YA199
        return 5
    elif ya < 300:                   # YA299
        return 3
    elif ya < 350:                   # YA349
        return 1
    elif ya < 450:                   # (zero bucket, no YA* stat)
        return 0
    elif ya < 500:                   # YA499
        return -1
    elif ya < 550:                   # YA549
        return -2
    else:                            # YA550
        return -3


def porchcrew_dst_points(row) -> float:
    """
    row: dict-like with team defensive stats for a single game/week.
    Expected keys (you can rename to taste):

      sacks               -> SK
      blocked_kicks       -> BLKK      (blocked punt/PAT/FG)
      interceptions       -> INT
      fumbles_recovered   -> FR
      safeties            -> SR

      int_td              -> INTTD
      fum_ret_td          -> FRTD
      kr_td               -> KRTD
      pr_td               -> PRTD
      blk_kick_td         -> BLKKRTD
      two_pt_returns      -> 2PTRET
      one_pt_safeties     -> 1PSF

      points_allowed      -> used for PA* buckets
      yards_allowed       -> used for YA* buckets
    """

    pts = 0.0

    # --- Base defensive events (ESPN defaults) ---
    pts += 2.0 * row.get("sacks", 0)              # SK
    pts += 2.0 * row.get("blocked_kicks", 0)      # BLKK
    pts += 2.0 * row.get("interceptions", 0)      # INT
    pts += 2.0 * row.get("fumbles_recovered", 0)  # FR
    pts += 5.0 * row.get("safeties", 0)           # SR

    # --- TDs & return stuff (your custom settings) ---
    pts += 6.0 * row.get("int_td", 0)             # INTTD
    pts += 6.0 * row.get("fum_ret_td", 0)         # FRTD
    pts += 6.0 * row.get("kr_td", 0)              # KRTD
    pts += 6.0 * row.get("pr_td", 0)              # PRTD
    pts += 6.0 * row.get("blk_kick_td", 0)        # BLKKRTD
    pts += 2.0 * row.get("two_pt_returns", 0)     # 2PTRET
    pts += 1.0 * row.get("one_pt_safeties", 0)    # 1PSF

    # --- Points allowed & yards allowed components ---
    pts += dst_points_allowed_component(row.get("points_allowed", 0))
    pts += dst_yards_allowed_component(row.get("yards_allowed", 0))

    return pts


# TD yard bonuses need to come from play-by-plays which is a different dataset

pbp = nfl.load_pbp(seasons=[2025])  # or seasons=True to grab all and then filter
pbp_2025 = pbp.filter(pl.col("season") == 2025)


# Concept only, exact column names not checked
plays = (
    pbp_2025
    .with_columns([
        # Passing TDs
        ((pl.col("pass") == 1) & (pl.col("touchdown") == 1) & (pl.col("yards_gained") >= 40))
            .cast(int).alias("ptd40_flag"),
        ((pl.col("pass") == 1) & (pl.col("touchdown") == 1) & (pl.col("yards_gained") >= 50))
            .cast(int).alias("ptd50_flag"),

        # Rushing TDs
        ((pl.col("rush_attempt") == 1) & (pl.col("touchdown") == 1) & (pl.col("yards_gained") >= 40))
            .cast(int).alias("rtd40_flag"),
        ((pl.col("rush_attempt") == 1) & (pl.col("touchdown") == 1) & (pl.col("yards_gained") >= 50))
            .cast(int).alias("rtd50_flag"),

        # Receiving TDs (pass play + receiving TD)
        ((pl.col("pass") == 1) & (pl.col("receiving_td") == 1) & (pl.col("yards_gained") >= 40))
            .cast(int).alias("retd40_flag"),
        ((pl.col("pass") == 1) & (pl.col("receiving_td") == 1) & (pl.col("yards_gained") >= 50))
            .cast(int).alias("retd50_flag"),
    ])
)

# Aggregate by players and week
qb_long_tds = (
    plays
    .groupby(["season", "week", "passer_id"])
    .agg([
        pl.col("ptd40_flag").sum().alias("pass_td_40_plus"),
        pl.col("ptd50_flag").sum().alias("pass_td_50_plus"),
    ])
)

rb_long_tds = (
    plays
    .groupby(["season", "week", "rusher_id"])
    .agg([
        pl.col("rtd40_flag").sum().alias("rush_td_40_plus"),
        pl.col("rtd50_flag").sum().alias("rush_td_50_plus"),
    ])
)

wr_long_tds = (
    plays
    .groupby(["season", "week", "receiver_id"])
    .agg([
        pl.col("retd40_flag").sum().alias("rec_td_40_plus"),
        pl.col("retd50_flag").sum().alias("rec_td_50_plus"),
    ])
)


stats = nfl.load_player_stats([2025], summary_level="week")

### TODO ###
# Got to join the stats, qb_long_tds, rb_long_tds, wr_long_tds on (season, week, player_id) to get an "all_stats" table


### TODO ###
# Create the "fantasy points" table by joining all_stats onto the results of the relevant scoring function (porchcrew_offense_points for QB, RB, WR, and TE positions | porchcrew_kicker_points for the K position)

### TODO ###
# The D/ST is a bit more difficult. We can use the precomputed team stats:
team_stats = nfl.load_team_stats(seasons=[2025])
# ...and filter to only the defense/special teams.
# Then, using the columns in this table (check the names), we can build a tidy stats table that looks like the following:
dst_weekly = (
    team_stats
    .select([
        "season", "week", "team",
        "points_allowed",        # whatever the actual column is
        "yards_allowed",
        "def_interceptions",
        "def_fumbles_recovered",
        "def_safeties",
    ])
)

### TODO ###
# Output the final table(s). We can have this be 1 table for all the players in the league and one for the D/ST (since it's aggregated over the whole team and calculated differently), or perhaps it makes more sense to have separate: QB, RB, WR/TE, K, D/ST tables.

# The final tables should include the week, team, player name, and then all of the raw stats that were used in the scoring calculations above. Then it should include the calculated "fantasy score".
