"""Rolling fatigue features with exponential time-decay.

Fatigue is the accumulated, time-decayed court load a player carries INTO a
match. Like Elo, it must be strictly backward-looking: a player's fatigue for
match i is computed only from matches with a date strictly earlier than i's.

Design:
  * We explode the match frame into a per-player "appearance" long frame
    (each match contributes two rows: winner and loser), each carrying the
    minutes played and the date.
  * For each appearance we sum, over prior appearances within ``window_days``,
    the minutes weighted by exp(-ln2 * age_days / half_life). An appearance
    on the same day as the target match but from an *earlier round* is a real
    prior load, so the comparison is strictly on date deltas > 0 plus an
    intra-day guard via match ordering.

Unlike Elo, the core reduction here IS vectorisable with a Polars rolling
join on the sorted long frame, so we keep it in-engine and fast. We still
guard the boundary carefully so no same-instant row leaks into its own window.
"""

from __future__ import annotations

import math

import polars as pl

from ..utils.config import FatigueConfig
from ..utils.logging import get_logger

log = get_logger()


class FatigueEngine:
    def __init__(self, config: FatigueConfig) -> None:
        self._cfg = config
        self._decay_lambda = math.log(2.0) / config.half_life_days

    def transform(self, df: pl.DataFrame) -> pl.DataFrame:
        """Augment ``df`` with ``w_fatigue`` and ``l_fatigue`` (pre-match)."""
        # Stamp a stable per-row uid onto df itself so fatigue rejoins to the
        # exact match slot. Appearances derive their uid from the same stamp.
        df = df.with_row_index("match_uid")

        appearances = self._to_appearances(df)
        fatigue = self._rolling_fatigue(appearances)  # cols: match_uid, player_id, slot, fatigue

        # slot 0 -> winner, slot 1 -> loser. Split and rejoin on (uid, player).
        w_fat = (
            fatigue.filter(pl.col("slot") == 0)
            .select("match_uid", pl.col("player_id").alias("winner_id"),
                    pl.col("fatigue").alias("w_fatigue"))
        )
        l_fat = (
            fatigue.filter(pl.col("slot") == 1)
            .select("match_uid", pl.col("player_id").alias("loser_id"),
                    pl.col("fatigue").alias("l_fatigue"))
        )

        out = (
            df.join(w_fat, on=["match_uid", "winner_id"], how="left")
              .join(l_fat, on=["match_uid", "loser_id"], how="left")
              .with_columns(
                  pl.col("w_fatigue").fill_null(0.0),
                  pl.col("l_fatigue").fill_null(0.0),
              )
              .with_columns(
                  (pl.col("w_fatigue") - pl.col("l_fatigue")).alias("fatigue_diff")
              )
              .drop("match_uid")
        )
        log.info("Fatigue engine processed %d appearances", appearances.height)
        return out

    def _to_appearances(self, df: pl.DataFrame) -> pl.DataFrame:
        # df already carries the stable per-match uid stamped in transform();
        # reuse it so fatigue rejoins to the exact slot.
        base = df

        minutes = (
            pl.col("minutes").fill_null(self._cfg.default_match_minutes)
            if "minutes" in df.columns
            else pl.lit(self._cfg.default_match_minutes)
        )

        winners = base.select(
            pl.col("match_uid"),
            pl.col("winner_id").alias("player_id"),
            pl.col("match_date"),
            minutes.alias("minutes_played"),
            pl.lit(0).alias("slot"),  # 0 = winner, 1 = loser; for stable order
        )
        losers = base.select(
            pl.col("match_uid"),
            pl.col("loser_id").alias("player_id"),
            pl.col("match_date"),
            minutes.alias("minutes_played"),
            pl.lit(1).alias("slot"),
        )

        appearances = pl.concat([winners, losers], how="vertical").sort(
            ["player_id", "match_date", "match_uid", "slot"]
        )
        return appearances

    def _rolling_fatigue(self, appearances: pl.DataFrame) -> pl.DataFrame:
        """Time-decayed sum of PRIOR minutes within the window, per appearance.

        Implemented as a self-join bounded by the window, filtering to strictly
        prior appearances. Correct-by-construction over a clever-but-fragile
        rolling aggregate: the self-join makes the "strictly before" boundary
        explicit, which is exactly where leakage bugs hide.
        """
        window = self._cfg.window_days
        lam = self._decay_lambda

        left = appearances.select(
            "match_uid", "player_id", "slot",
            pl.col("match_date").alias("t_date"),
        )
        right = appearances.select(
            "player_id",
            pl.col("match_date").alias("p_date"),
            pl.col("match_uid").alias("p_uid"),
            "minutes_played",
        )

        joined = left.join(right, on="player_id", how="left")

        # Keep only prior appearances within the window. "Prior" means an
        # earlier date, OR the same date but a strictly earlier match_uid
        # (earlier round that day). Never the appearance itself.
        age_days = (pl.col("t_date") - pl.col("p_date")).dt.total_days()
        is_prior = (age_days > 0) | ((age_days == 0) & (pl.col("p_uid") < pl.col("match_uid")))
        in_window = age_days <= window

        decayed = (
            pl.col("minutes_played")
            * (-lam * (pl.col("t_date") - pl.col("p_date")).dt.total_days()).exp()
        ).alias("decayed")

        contrib = (
            joined.filter(is_prior & in_window)
            .with_columns(decayed)
            .group_by("match_uid", "player_id", "slot")
            .agg(pl.col("decayed").sum().alias("fatigue"))
        )

        # Reattach zero-fatigue appearances (players with no prior load).
        result = (
            appearances.select("match_uid", "player_id", "slot")
            .join(contrib, on=["match_uid", "player_id", "slot"], how="left")
            .with_columns(pl.col("fatigue").fill_null(0.0))
        )
        return result
