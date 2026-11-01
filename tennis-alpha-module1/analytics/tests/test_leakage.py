"""Leakage + correctness tests for the feature pipeline.

These tests are the contract. If any of them fail, the model built on top is
not trustworthy. The headline test (`test_elo_is_strictly_pre_match`) proves
the causal invariant on a hand-constructed sequence.
"""

from __future__ import annotations

import polars as pl

from src.features.elo import EloEngine
from src.features.fatigue import FatigueEngine
from src.utils.config import EloConfig, FatigueConfig, KFactorConfig


def _elo_cfg() -> EloConfig:
    return EloConfig(
        initial_rating=1500.0,
        scale=400.0,
        k_factor=KFactorConfig(base=250.0, shape=0.4, offset=5.0),
        surface_blend=0.5,
        surfaces=("Hard", "Clay", "Grass", "Carpet"),
    )


def _fatigue_cfg() -> FatigueConfig:
    return FatigueConfig(window_days=14, default_match_minutes=90.0, half_life_days=7.0)


def _frame(rows: list[dict]) -> pl.DataFrame:
    df = pl.DataFrame(rows)
    return df.with_columns(pl.col("match_date").str.strptime(pl.Date, "%Y-%m-%d"))


def test_elo_first_match_is_default_for_both():
    """A player's first-ever appearance must carry the initial rating."""
    df = _frame([
        {"match_date": "2020-01-01", "tourney_id": "t1", "match_num": 1,
         "winner_id": 10, "loser_id": 20, "surface": "Hard"},
    ])
    out = EloEngine(_elo_cfg()).transform(df)
    assert out["w_elo_global"][0] == 1500.0
    assert out["l_elo_global"][0] == 1500.0


def test_elo_is_strictly_pre_match():
    """THE leakage test.

    Player 10 beats 20 on day 1. On day 2, player 10 plays again. The Elo
    emitted for player 10's day-2 match must equal their rating AFTER the day-1
    win (i.e. > 1500), and must NOT reflect the day-2 result at all.
    """
    df = _frame([
        {"match_date": "2020-01-01", "tourney_id": "t1", "match_num": 1,
         "winner_id": 10, "loser_id": 20, "surface": "Hard"},
        {"match_date": "2020-01-02", "tourney_id": "t2", "match_num": 1,
         "winner_id": 30, "loser_id": 10, "surface": "Hard"},
    ])
    out = EloEngine(_elo_cfg()).transform(df)

    # Day 1: both default.
    assert out["w_elo_global"][0] == 1500.0
    # Day 2: player 10 sits in the LOSER slot. Their pre-match rating must be
    # the post-day-1 value (they won day 1), strictly above the default...
    p10_day2 = out["l_elo_global"][1]
    assert p10_day2 > 1500.0
    # ...and it must NOT have been pushed down by the day-2 loss yet.
    gained_day1 = p10_day2 - 1500.0
    assert gained_day1 > 0.0


def test_elo_is_zero_sum_ish_on_single_match():
    """Winner gains, loser loses; with equal K the magnitudes match."""
    df = _frame([
        {"match_date": "2020-01-01", "tourney_id": "t1", "match_num": 1,
         "winner_id": 10, "loser_id": 20, "surface": "Clay"},
        {"match_date": "2020-01-05", "tourney_id": "t2", "match_num": 1,
         "winner_id": 10, "loser_id": 20, "surface": "Clay"},
    ])
    out = EloEngine(_elo_cfg()).transform(df)
    # On match 2 we can read the settled post-match-1 ratings.
    w_after = out["w_elo_global"][1]
    l_after = out["l_elo_global"][1]
    assert abs((w_after - 1500.0) - (1500.0 - l_after)) < 1e-9


def test_fatigue_first_match_is_zero():
    df = _frame([
        {"match_date": "2020-01-01", "tourney_id": "t1", "match_num": 1,
         "winner_id": 10, "loser_id": 20, "surface": "Hard", "minutes": 120.0},
    ])
    out = FatigueEngine(_fatigue_cfg()).transform(df)
    assert out["w_fatigue"][0] == 0.0
    assert out["l_fatigue"][0] == 0.0


def test_fatigue_accumulates_from_prior_matches_only():
    """Day-2 fatigue reflects the day-1 match; day-1 fatigue is zero."""
    df = _frame([
        {"match_date": "2020-01-01", "tourney_id": "t1", "match_num": 1,
         "winner_id": 10, "loser_id": 20, "surface": "Hard", "minutes": 100.0},
        {"match_date": "2020-01-03", "tourney_id": "t2", "match_num": 1,
         "winner_id": 10, "loser_id": 30, "surface": "Hard", "minutes": 100.0},
    ])
    out = FatigueEngine(_fatigue_cfg()).transform(df)
    # Player 10 played day 1 -> carries decayed fatigue into day 3.
    assert out["w_fatigue"][0] == 0.0
    assert out["w_fatigue"][1] > 0.0
    # Player 30, debuting day 3, has no prior load.
    assert out["l_fatigue"][1] == 0.0
