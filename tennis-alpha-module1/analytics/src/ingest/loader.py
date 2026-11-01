"""Raw-data ingestion for Jeff Sackmann match CSVs.

Responsibilities (and nothing more):
  1. Discover and concatenate per-year CSVs under the raw dir.
  2. Enforce a strict schema (Sackmann columns drift slightly across eras).
  3. Parse the integer ``tourney_date`` (YYYYMMDD) into a real Date.
  4. Drop structurally-unusable rows and normalise the surface taxonomy.

This module is deliberately label-agnostic and leakage-agnostic: it does NOT
compute anything derived. It hands clean, chronologically-sortable raw rows
to the feature layer. Keeping ingestion pure makes the whole pipeline
reproducible and unit-testable.
"""

from __future__ import annotations

from pathlib import Path

import polars as pl

from ..utils.config import Settings
from ..utils.logging import get_logger

log = get_logger()

# Subset of the Sackmann schema we actually consume. Anything outside this
# is dropped at read time to keep memory bounded on multi-decade loads.
# Columns are read as Utf8 first, then cast explicitly, because Sackmann
# files mix empty strings and integers in numeric columns across years.
_SCHEMA: dict[str, pl.DataType] = {
    "tourney_id": pl.Utf8,
    "tourney_name": pl.Utf8,
    "surface": pl.Utf8,
    "tourney_level": pl.Utf8,
    "tourney_date": pl.Int64,
    "match_num": pl.Int64,
    "winner_id": pl.Int64,
    "winner_name": pl.Utf8,
    "winner_hand": pl.Utf8,
    "winner_rank": pl.Float64,
    "loser_id": pl.Int64,
    "loser_name": pl.Utf8,
    "loser_hand": pl.Utf8,
    "loser_rank": pl.Float64,
    "score": pl.Utf8,
    "best_of": pl.Int64,
    "round": pl.Utf8,
    "minutes": pl.Float64,
}

# Canonical surface names. Sackmann is mostly clean but stray casing and
# blanks appear; anything unmapped becomes "Unknown" and is excluded from
# surface-specific Elo while still contributing to global Elo.
_SURFACE_MAP: dict[str, str] = {
    "hard": "Hard",
    "clay": "Clay",
    "grass": "Grass",
    "carpet": "Carpet",
}


def discover_files(settings: Settings) -> list[Path]:
    raw_dir = settings.paths.raw_dir
    if not raw_dir.is_dir():
        raise FileNotFoundError(f"Raw data directory does not exist: {raw_dir}")

    files = sorted(raw_dir.glob(settings.ingest.file_glob))
    if not files:
        raise FileNotFoundError(
            f"No files matching '{settings.ingest.file_glob}' in {raw_dir}. "
            "Expected Sackmann CSVs, e.g. atp_matches_2023.csv"
        )
    log.info("Discovered %d raw CSV file(s) in %s", len(files), raw_dir)
    return files


def _read_one(path: Path) -> pl.DataFrame:
    """Read a single CSV, tolerating column drift via a projected schema."""
    # scan_csv with infer_schema_length=0 forces all-Utf8, dodging the
    # mixed-type parse errors Sackmann files throw on numeric blanks.
    lazy = pl.scan_csv(path, infer_schema_length=0, ignore_errors=True)
    available = set(lazy.collect_schema().names())

    projections: list[pl.Expr] = []
    for col, dtype in _SCHEMA.items():
        if col in available:
            projections.append(pl.col(col).cast(dtype, strict=False).alias(col))
        else:
            # Column absent this era -> materialise a typed null column so
            # every yearly frame shares one schema and concats cleanly.
            projections.append(pl.lit(None, dtype=dtype).alias(col))

    return lazy.select(projections).collect()


def load_raw(settings: Settings) -> pl.DataFrame:
    """Load, concatenate, clean and chronologically order all raw matches."""
    files = discover_files(settings)

    frames = [_read_one(p) for p in files]
    df = pl.concat(frames, how="vertical_relaxed")
    log.info("Concatenated raw rows: %d", df.height)

    df = _parse_dates(df)
    df = _filter_rows(df, settings)
    df = _normalise_surface(df)

    # Stable chronological order is the backbone of leakage-free feature
    # engineering. Ties within a day are broken by match_num so the Elo
    # walk is deterministic and reproducible across runs.
    df = df.sort(["tourney_date", "tourney_id", "match_num"], nulls_last=True)

    log.info("Clean raw rows after filtering: %d", df.height)
    return df


def _parse_dates(df: pl.DataFrame) -> pl.DataFrame:
    # Sackmann encodes dates as the integer YYYYMMDD. Convert to Utf8 then
    # to a proper Date so downstream rolling windows use real day deltas.
    return df.with_columns(
        pl.col("tourney_date")
        .cast(pl.Utf8)
        .str.strptime(pl.Date, format="%Y%m%d", strict=False)
        .alias("match_date")
    )


def _filter_rows(df: pl.DataFrame, settings: Settings) -> pl.DataFrame:
    before = df.height

    # Drop rows missing any structurally-required field.
    required = [c for c in settings.ingest.required_columns if c != "tourney_date"]
    df = df.drop_nulls(subset=["match_date", *required])

    # Year floor.
    df = df.filter(pl.col("match_date").dt.year() >= settings.ingest.min_year)

    # Walkovers / retirements with no real play distort fatigue minutes but
    # are legitimate results for Elo, so we keep them and only null their
    # minutes if the score string flags a walkover.
    df = df.with_columns(
        pl.when(pl.col("score").str.contains("(?i)w/o|walkover"))
        .then(None)
        .otherwise(pl.col("minutes"))
        .alias("minutes")
    )

    log.info("Row filter dropped %d rows (%d -> %d)", before - df.height, before, df.height)
    return df


def _normalise_surface(df: pl.DataFrame) -> pl.DataFrame:
    return df.with_columns(
        pl.col("surface")
        .str.strip_chars()
        .str.to_lowercase()
        .replace_strict(_SURFACE_MAP, default="Unknown")
        .alias("surface")
    )
