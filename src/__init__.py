# src/__init__.py
"""
mybanketl – a tiny library that extracts, transforms, loads and reconciles
bank‑transaction data.

The package does **not** configure logging automatically – that would interfere
with an application that already has its own logging configuration.  Instead
we expose a small helper (`configure_logging`) that can be called once by the
consumer (CLI script, Airflow DAG, Jupyter notebook, etc.).  If the consumer
doesn’t call it, we still attach a `NullHandler` so that “No handler found”
warnings are avoided.
"""

from __future__ import annotations

import logging

# 1️.Export a module‑level logger that every sub‑module can reuse

logger = logging.getLogger(__name__)          # e.g. "src"
logger.addHandler(logging.NullHandler())      # safe default – no output unless configured

# 2.Helper to configure a simple console logger 
def configure_logging(level: int = logging.INFO) -> None:
    """
    Configure a basic ``logging.basicConfig`` for the whole package if the
    application has not already configured logging.

    Parameters
    level : int, optional
        Logging level to use (default ``logging.INFO``).
    """
    root = logging.getLogger()
    if not root.handlers:                # only configure once
        logging.basicConfig(
            level=level,
            format="%(asctime)s | %(levelname)-8s | %(name)s | %(message)s",
            datefmt="%Y-%m-%d %H:%M:%S",
        )
    # Ensure the package logger propagates to the root handler
    logger.setLevel(level)