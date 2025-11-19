"""Scoring functions for Porch Crew fantasy league."""

from typing import Dict, Any


def porchcrew_offense_points(row: Dict[str, Any]) -> float:
    """
    Calculate fantasy points for offensive players (QB, RB, WR, TE).

    Args:
        row: A dict-like object (e.g. pandas Series) with nflverse offensive columns.

    Returns:
        Fantasy points for the player based on Porch Crew scoring rules.
        Note: Long-TD bonuses are included if pass_td_40p/pass_td_50p,
        rush_td_40p/rush_td_50p, or rec_td_40p/rec_td_50p fields are provided.
    """
    pts = 0.0

    # --- Passing ---
    pts += 0.04 * row.get("passing_yards", 0)
    pts += 6.0 * row.get("passing_tds", 0)
    pts += -3.0 * row.get("passing_interceptions", 0)
    pts += 2.0 * row.get("passing_2pt_conversions", 0)

    # Optional: long passing TD bonuses (if you precompute these)
    pts += 2.0 * row.get("pass_td_40p", 0)
    pts += 3.0 * row.get("pass_td_50p", 0)

    # --- Rushing ---
    pts += 0.10 * row.get("rushing_yards", 0)
    pts += 6.0 * row.get("rushing_tds", 0)
    pts += 2.0 * row.get("rushing_2pt_conversions", 0)

    pts += 2.0 * row.get("rush_td_40p", 0)  # optional
    pts += 3.0 * row.get("rush_td_50p", 0)  # optional

    # --- Receiving ---
    pts += 0.10 * row.get("receiving_yards", 0)
    pts += 1.0 * row.get("receptions", 0)
    pts += 6.0 * row.get("receiving_tds", 0)
    pts += 2.0 * row.get("receiving_2pt_conversions", 0)

    pts += 2.0 * row.get("rec_td_40p", 0)  # optional
    pts += 3.0 * row.get("rec_td_50p", 0)  # optional

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


def porchcrew_kicker_points(row: Dict[str, Any]) -> float:
    """
    Calculate fantasy points for kickers.

    Args:
        row: A dict-like object with nflverse kicking columns (from stat_type='kicking').

    Returns:
        Fantasy points for the kicker based on Porch Crew scoring rules.
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
    miss_0_19 = row.get("fg_missed_0_19", 0)
    miss_20_29 = row.get("fg_missed_20_29", 0)
    miss_30_39 = row.get("fg_missed_30_39", 0)
    miss_40_49 = row.get("fg_missed_40_49", 0)
    miss_50_59 = row.get("fg_missed_50_59", 0)
    miss_60 = row.get("fg_missed_60_", 0)

    total_missed = row.get(
        "fg_missed",
        miss_0_19 + miss_20_29 + miss_30_39 + miss_40_49 + miss_50_59 + miss_60,
    )

    # -1 for every miss
    pts += -1.0 * total_missed

    # Extra penalties per your settings
    miss_0_39_total = miss_0_19 + miss_20_29 + miss_30_39
    pts += -3.0 * miss_0_39_total  # -3 more → net -4 each
    pts += -2.0 * miss_40_49  # -2 more → net -3 each
    # 50+ only get the -1 generic

    return pts


def dst_points_allowed_component(points_allowed: int) -> int:
    """
    Calculate points allowed component for D/ST scoring using ESPN LM PA* buckets.

    Args:
        points_allowed: Points allowed by the defense.

    Returns:
        Points awarded/penalized based on points allowed bucket.
    """
    pa = points_allowed

    if pa == 0:  # PA0
        return 8
    elif 1 <= pa <= 6:  # PA1
        return 4
    elif 7 <= pa <= 13:  # PA7
        return 3
    elif 14 <= pa <= 17:  # PA14
        return 1
    elif 18 <= pa <= 27:  # (no PA code; 0 pts)
        return 0
    elif 28 <= pa <= 34:  # PA28
        return -1
    elif 35 <= pa <= 45:  # PA35
        return -3
    else:  # pa >= 46  -> PA46
        return -5


def dst_yards_allowed_component(yards_allowed: int) -> int:
    """
    Calculate yards allowed component for D/ST scoring using ESPN LM YA* buckets.

    Args:
        yards_allowed: Yards allowed by the defense.

    Returns:
        Points awarded/penalized based on yards allowed bucket.
    """
    ya = yards_allowed

    if ya < 100:  # YA100
        return 8
    elif ya < 200:  # YA199
        return 5
    elif ya < 300:  # YA299
        return 3
    elif ya < 350:  # YA349
        return 1
    elif ya < 450:  # (zero bucket, no YA* stat)
        return 0
    elif ya < 500:  # YA499
        return -1
    elif ya < 550:  # YA549
        return -2
    else:  # YA550
        return -3


def porchcrew_dst_points(row: Dict[str, Any]) -> float:
    """
    Calculate fantasy points for Defense/Special Teams.

    Args:
        row: A dict-like object with team defensive stats for a single game/week.
            Expected keys:
            - sacks: Sacks
            - blocked_kicks: Blocked punt/PAT/FG
            - interceptions: Interceptions
            - fumbles_recovered: Fumbles recovered
            - safeties: Safeties
            - int_td: Interception return TDs
            - fum_ret_td: Fumble return TDs
            - kr_td: Kick return TDs
            - pr_td: Punt return TDs
            - blk_kick_td: Blocked kick return TDs
            - two_pt_returns: Two-point conversion returns
            - one_pt_safeties: One-point safeties
            - points_allowed: Points allowed (for PA* buckets)
            - yards_allowed: Yards allowed (for YA* buckets)

    Returns:
        Fantasy points for the D/ST based on Porch Crew scoring rules.
    """
    pts = 0.0

    # --- Base defensive events (ESPN defaults) ---
    pts += 2.0 * row.get("sacks", 0)  # SK
    pts += 2.0 * row.get("blocked_kicks", 0)  # BLKK
    pts += 2.0 * row.get("interceptions", 0)  # INT
    pts += 2.0 * row.get("fumbles_recovered", 0)  # FR
    pts += 5.0 * row.get("safeties", 0)  # SR

    # --- TDs & return stuff (your custom settings) ---
    pts += 6.0 * row.get("int_td", 0)  # INTTD
    pts += 6.0 * row.get("fum_ret_td", 0)  # FRTD
    pts += 6.0 * row.get("kr_td", 0)  # KRTD
    pts += 6.0 * row.get("pr_td", 0)  # PRTD
    pts += 6.0 * row.get("blk_kick_td", 0)  # BLKKRTD
    pts += 2.0 * row.get("two_pt_returns", 0)  # 2PTRET
    pts += 1.0 * row.get("one_pt_safeties", 0)  # 1PSF

    # --- Points allowed & yards allowed components ---
    pts += dst_points_allowed_component(row.get("points_allowed", 0))
    pts += dst_yards_allowed_component(row.get("yards_allowed", 0))

    return pts


