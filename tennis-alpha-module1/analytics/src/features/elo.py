"""Surface-aware Elo engine with strict temporal causality.

THE LEAKAGE RULE (read this before touching anything):
---------------------------------------------------------------------------
For every match we emit the ratings of both players *as they stood BEFORE
the match was played*, then we apply the result to update state. A feature
row therefore never contains information that post-dates its own match.
This single invariant is what separates a model that backtests at 68% and
lives at 51%, from one whose backtest is honest.
---------------------------------------------------------------------------

Why a Python loop and not a vectorised Polars op:
Elo is a recurrence relation — rating[t+1] depends on rating[t]. There is no
leakage-safe way to vectorise it across the match axis, because every update
must see the fully-settled state of all prior matches. We pay one sequential
pass over the data (O(n), dict lookups) and in return get provably-causal
features. On a full ATP history (~190k matches) this is a few seconds.

Two rating tracks per player are maintained simultaneously:
  * global Elo   — all surfaces pooled
  * surface Elo  — one independent ladder per surface

The emitted pre-match strength is a convex blend of the two (``surface_blend``),
which captures both a player's overall level and their surface specialism.
"""

from __future__ import annotations

from dataclasses import dataclass, field

import polars as pl

from ..utils.config import EloConfig
from ..utils.logging import get_logger

log = get_logger()


@dataclass(slots=True)
class _PlayerState:
    """Mutable per-player rating state. Lives only inside the engine."""

    global_rating: float
    global_matches: int = 0
    surface_ratings: dict[str, float] = field(default_factory=dict)
    surface_matches: dict[str, int] = field(default_factory=dict)


class EloEngine:
    """Single-pass, causal Elo computation over a chronologically sorted frame."""

    def __init__(self, config: EloConfig) -> None:
        self._cfg = config
        self._state: dict[int, _PlayerState] = {}

    # -- public API ---------------------------------------------------------

    def transform(self, df: pl.DataFrame) -> pl.DataFrame:
        """Return ``df`` augmented with pre-match Elo feature columns.

        Added columns (all strictly pre-match, i.e. leakage-free):
          w_elo_global, l_elo_global
          w_elo_surface, l_elo_surface
          w_elo_blend, l_elo_blend
          elo_blend_diff        (w_blend - l_blend; the headline signal)
          w_global_matches, l_global_matches   (experience -> drives K)
        """
        self._validate_sorted(df)

        n = df.height
        cols = {
            "w_elo_global": [0.0] * n,
            "l_elo_global": [0.0] * n,
            "w_elo_surface": [0.0] * n,
            "l_elo_surface": [0.0] * n,
            "w_elo_blend": [0.0] * n,
            "l_elo_blend": [0.0] * n,
            "w_global_matches": [0] * n,
            "l_global_matches": [0] * n,
        }

        winners = df["winner_id"].to_list()
        losers = df["loser_id"].to_list()
        surfaces = df["surface"].to_list()

        for i in range(n):
            w_id, l_id, surface = winners[i], losers[i], surfaces[i]

            w_state = self._get_or_create(w_id)
            l_state = self._get_or_create(l_id)

            # --- SNAPSHOT: read ratings BEFORE the match (causal emit) -----
            w_global = w_state.global_rating
            l_global = l_state.global_rating
            w_surf = self._surface_rating(w_state, surface)
            l_surf = self._surface_rating(l_state, surface)
            w_blend = self._blend(w_global, w_surf, surface)
            l_blend = self._blend(l_global, l_surf, surface)

            cols["w_elo_global"][i] = w_global
            cols["l_elo_global"][i] = l_global
            cols["w_elo_surface"][i] = w_surf
            cols["l_elo_surface"][i] = l_surf
            cols["w_elo_blend"][i] = w_blend
            cols["l_elo_blend"][i] = l_blend
            cols["w_global_matches"][i] = w_state.global_matches
            cols["l_global_matches"][i] = l_state.global_matches

            # --- UPDATE: apply the result to settle state for match i+1 ----
            self._apply_result(w_state, l_state, surface)

        out = df.with_columns([pl.Series(name, values) for name, values in cols.items()])
        out = out.with_columns(
            (pl.col("w_elo_blend") - pl.col("l_elo_blend")).alias("elo_blend_diff")
        )
        log.info("Elo engine processed %d matches; %d unique players tracked",
                 n, len(self._state))
        return out

    # -- internals ----------------------------------------------------------

    def _get_or_create(self, player_id: int) -> _PlayerState:
        state = self._state.get(player_id)
        if state is None:
            state = _PlayerState(global_rating=self._cfg.initial_rating)
            self._state[player_id] = state
        return state

    def _surface_rating(self, state: _PlayerState, surface: str) -> float:
        # "Unknown" surface players fall back to their global rating so the
        # blend degrades gracefully instead of injecting the default 1500.
        if surface == "Unknown":
            return state.global_rating
        return state.surface_ratings.get(surface, self._cfg.initial_rating)

    def _blend(self, global_r: float, surface_r: float, surface: str) -> float:
        if surface == "Unknown":
            return global_r
        b = self._cfg.surface_blend
        return b * surface_r + (1.0 - b) * global_r

    def _k_factor(self, match_count: int) -> float:
        # Decaying K: high when a player has few matches (rating uncertain),
        # asymptotically small once established. K = base / (matches + offset)^shape
        kf = self._cfg.k_factor
        return kf.base / ((match_count + kf.offset) ** kf.shape)

    def _expected(self, rating_a: float, rating_b: float) -> float:
        # Logistic expectation that A beats B.
        return 1.0 / (1.0 + 10.0 ** ((rating_b - rating_a) / self._cfg.scale))

    def _apply_result(self, winner: _PlayerState, loser: _PlayerState, surface: str) -> None:
        # ---- global ladder ----
        exp_w = self._expected(winner.global_rating, loser.global_rating)
        k_w = self._k_factor(winner.global_matches)
        k_l = self._k_factor(loser.global_matches)
        # Winner scored 1, loser scored 0. Rating change is symmetric in the
        # surprise term (1 - exp_w), scaled by each player's own K.
        winner.global_rating += k_w * (1.0 - exp_w)
        loser.global_rating += k_l * (0.0 - (1.0 - exp_w))
        winner.global_matches += 1
        loser.global_matches += 1

        # ---- surface ladder (skip Unknown to avoid polluting real ladders) ----
        if surface == "Unknown":
            return
        w_surf = winner.surface_ratings.get(surface, self._cfg.initial_rating)
        l_surf = loser.surface_ratings.get(surface, self._cfg.initial_rating)
        w_cnt = winner.surface_matches.get(surface, 0)
        l_cnt = loser.surface_matches.get(surface, 0)

        exp_ws = self._expected(w_surf, l_surf)
        winner.surface_ratings[surface] = w_surf + self._k_factor(w_cnt) * (1.0 - exp_ws)
        loser.surface_ratings[surface] = l_surf + self._k_factor(l_cnt) * (-(1.0 - exp_ws))
        winner.surface_matches[surface] = w_cnt + 1
        loser.surface_matches[surface] = l_cnt + 1

    @staticmethod
    def _validate_sorted(df: pl.DataFrame) -> None:
        if df.height <= 1:
            return
        dates = df["match_date"]
        if not dates.is_sorted():
            raise ValueError(
                "EloEngine requires a chronologically sorted frame. "
                "Sort by [match_date, tourney_id, match_num] before transform()."
            )
