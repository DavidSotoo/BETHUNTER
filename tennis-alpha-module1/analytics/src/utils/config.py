"""Typed configuration loader.

Parses ``config/settings.yaml`` into frozen dataclasses so the rest of the
pipeline consumes strongly-typed objects rather than loose dicts. Fail fast
on malformed config: a bad K-factor here is cheaper to catch than a silent
NaN in a feature column three stages downstream.
"""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any

import yaml


@dataclass(frozen=True, slots=True)
class PathsConfig:
    raw_dir: Path
    processed_dir: Path
    models_dir: Path
    feature_store: Path


@dataclass(frozen=True, slots=True)
class IngestConfig:
    file_glob: str
    min_year: int
    required_columns: tuple[str, ...]


@dataclass(frozen=True, slots=True)
class KFactorConfig:
    base: float
    shape: float
    offset: float


@dataclass(frozen=True, slots=True)
class EloConfig:
    initial_rating: float
    scale: float
    k_factor: KFactorConfig
    surface_blend: float
    surfaces: tuple[str, ...]


@dataclass(frozen=True, slots=True)
class FatigueConfig:
    window_days: int
    default_match_minutes: float
    half_life_days: float


@dataclass(frozen=True, slots=True)
class OutputConfig:
    drop_raw_after_features: bool
    float_precision: int


@dataclass(frozen=True, slots=True)
class Settings:
    paths: PathsConfig
    ingest: IngestConfig
    elo: EloConfig
    fatigue: FatigueConfig
    output: OutputConfig
    project_root: Path

    @staticmethod
    def load(config_path: str | Path, project_root: str | Path | None = None) -> "Settings":
        config_path = Path(config_path).resolve()
        if not config_path.is_file():
            raise FileNotFoundError(f"Config file not found: {config_path}")

        root = Path(project_root).resolve() if project_root else config_path.parent.parent

        with config_path.open("r", encoding="utf-8") as fh:
            raw: dict[str, Any] = yaml.safe_load(fh)

        _require_keys(raw, {"paths", "ingest", "elo", "fatigue", "output"}, "root")

        paths = PathsConfig(
            raw_dir=root / raw["paths"]["raw_dir"],
            processed_dir=root / raw["paths"]["processed_dir"],
            models_dir=root / raw["paths"]["models_dir"],
            feature_store=root / raw["paths"]["feature_store"],
        )

        ingest = IngestConfig(
            file_glob=raw["ingest"]["file_glob"],
            min_year=int(raw["ingest"]["min_year"]),
            required_columns=tuple(raw["ingest"]["required_columns"]),
        )

        kf = raw["elo"]["k_factor"]
        elo = EloConfig(
            initial_rating=float(raw["elo"]["initial_rating"]),
            scale=float(raw["elo"]["scale"]),
            k_factor=KFactorConfig(
                base=float(kf["base"]),
                shape=float(kf["shape"]),
                offset=float(kf["offset"]),
            ),
            surface_blend=float(raw["elo"]["surface_blend"]),
            surfaces=tuple(raw["elo"]["surfaces"]),
        )
        if not 0.0 <= elo.surface_blend <= 1.0:
            raise ValueError(f"elo.surface_blend must be in [0, 1], got {elo.surface_blend}")

        fatigue = FatigueConfig(
            window_days=int(raw["fatigue"]["window_days"]),
            default_match_minutes=float(raw["fatigue"]["default_match_minutes"]),
            half_life_days=float(raw["fatigue"]["half_life_days"]),
        )
        if fatigue.half_life_days <= 0:
            raise ValueError("fatigue.half_life_days must be > 0")

        output = OutputConfig(
            drop_raw_after_features=bool(raw["output"]["drop_raw_after_features"]),
            float_precision=int(raw["output"]["float_precision"]),
        )

        return Settings(
            paths=paths,
            ingest=ingest,
            elo=elo,
            fatigue=fatigue,
            output=output,
            project_root=root,
        )


def _require_keys(mapping: dict[str, Any], keys: set[str], ctx: str) -> None:
    missing = keys - mapping.keys()
    if missing:
        raise KeyError(f"Missing config keys under '{ctx}': {sorted(missing)}")
