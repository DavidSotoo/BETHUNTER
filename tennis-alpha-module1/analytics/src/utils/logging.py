"""Structured logging setup.

A single configured logger for the whole pipeline. Timestamps + stage names
so that a long ETL run leaves an auditable trail — important when a feature
value later looks wrong and you need to know which stage produced it.
"""

from __future__ import annotations

import logging
import sys


def get_logger(name: str = "tennis_alpha", level: int = logging.INFO) -> logging.Logger:
    logger = logging.getLogger(name)
    if logger.handlers:  # idempotent: avoid duplicate handlers on re-import
        return logger

    logger.setLevel(level)
    handler = logging.StreamHandler(sys.stdout)
    formatter = logging.Formatter(
        fmt="%(asctime)s | %(levelname)-7s | %(name)s | %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )
    handler.setFormatter(formatter)
    logger.addHandler(handler)
    logger.propagate = False
    return logger
