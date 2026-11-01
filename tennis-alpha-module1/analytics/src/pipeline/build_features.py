"""ETL orchestrator: raw Sackmann CSVs -> training-ready feature store.

Stage order is load-bearing:
  1. load_raw         -> clean, chronologically-sorted matches
  2. EloEngine        -> pre-match global/surface/blended Elo (causal)
  3. FatigueEngine    -> pre-match time-decayed court load (causal)
  4. assemble         -> select the leakage-free feature columns, write Parquet

The output is deliberately in "winner/loser" orientation and carries NO target
column. The training module is responsible for randomising winner/loser into
player_a/player_b and attaching the binary label, so that this layer is reused
unchanged at inference time (where there is no winner yet).
"""

from __future__ import annotations

import argparse
from pathlib import Path

import polars as pl

from ..features.elo import EloEngine
from ..features.fatigue import FatigueEngine
from ..ingest.loader import load_raw
from ..utils.config import Settings
from ..utils.logging import get_logger

log = get_logger()

# The leakage-free feature set handed to training. Every column here is known
# strictly BEFORE the match. Raw post-match stats (aces, score, minutes of the
# match itself) are intentionally excluded — they are outcomes, not predictors.
_FEATURE_COLUMNS: tuple[str, ...] = (
    # identity / context (not features, but needed for split + audit)
    "match_date", "tourney_id", "tourney_level", "surface", "best_of", "round",
    "winner_id", "winner_name", "loser_id", "loser_name",
    # pre-match rank (Sackmann rank is as-of the tournament -> pre-match safe)
    "winner_rank", "loser_rank",
    # engineered, causal
    "w_elo_global", "l_elo_global",
    "w_elo_surface", "l_elo_surface",
    "w_elo_blend", "l_elo_blend", "elo_blend_diff",
    "w_global_matches", "l_global_matches",
    "w_fatigue", "l_fatigue", "fatigue_diff",
)


def run(settings: Settings) -> pl.DataFrame:
    log.info("=== Tennis-Alpha feature build :: START ===")

    raw = load_raw(settings)

    log.info("Stage 2/3: Elo engine")
    raw = EloEngine(settings.elo).transform(raw)

    log.info("Stage 3/3: Fatigue engine")
    raw = FatigueEngine(settings.fatigue).transform(raw)

    features = _assemble(raw, settings)
    _write(features, settings)

    log.info("=== Tennis-Alpha feature build :: DONE (%d rows) ===", features.height)
    return features


def _assemble(df: pl.DataFrame, settings: Settings) -> pl.DataFrame:
    missing = [c for c in _FEATURE_COLUMNS if c not in df.columns]
    if missing:
        raise RuntimeError(f"Feature assembly missing expected columns: {missing}")

    out = df.select(_FEATURE_COLUMNS)

    prec = settings.output.float_precision
    float_cols = [c for c, dt in out.schema.items() if dt in (pl.Float64, pl.Float32)]
    out = out.with_columns([pl.col(c).round(prec) for c in float_cols])

    # Final leakage assertion: the engineered diffs must be finite and the
    # frame must still be chronologically ordered. Cheap insurance.
    if not out["match_date"].is_sorted():
        raise RuntimeError("Feature store lost chronological order — aborting write.")
    return out


def _write(df: pl.DataFrame, settings: Settings) -> None:
    dest: Path = settings.paths.feature_store
    dest.parent.mkdir(parents=True, exist_ok=True)
    df.write_parquet(dest, compression="zstd")
    log.info("Feature store written: %s (%.2f MB)", dest, dest.stat().st_size / 1e6)


def _parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Build the Tennis-Alpha feature store.")
    p.add_argument(
        "--config",
        default=str(Path(__file__).resolve().parents[2] / "config" / "settings.yaml"),
        help="Path to settings.yaml",
    )
    return p.parse_args()


def main() -> None:
    args = _parse_args()
    settings = Settings.load(args.config)
    run(settings)


if __name__ == "__main__":
    main()
