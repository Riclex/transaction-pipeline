"""Checkpoint and resume functionality for the ETL pipeline

This module provides utilities to save pipeline state at checkpoints and
resume execution from the last successful checkpoint in case of failures
"""

from __future__ import annotations

import json
import logging
import pickle
from pathlib import Path
from typing import Dict, Any, Optional, Tuple
import pandas as pd

# Logging
LOGGER = logging.getLogger(__name__)


class PipelineCheckpoint:
    """Manages pipeline checkpoints for resume capability."""

    def __init__(self, checkpoint_dir: str | Path = "data/checkpoints"):
        """
        Initialize the checkpoint manager

        Parameters
        ----------
        checkpoint_dir : str | Path
            Directory to store checkpoint files
        """
        self.checkpoint_dir = Path(checkpoint_dir)
        self.checkpoint_dir.mkdir(parents=True, exist_ok=True)
        self.checkpoint_file = self.checkpoint_dir / "pipeline_checkpoint.json"
        self.state_file = self.checkpoint_dir / "pipeline_state.pkl"

    def save_checkpoint(
        self,
        step: str,
        data: Optional[Dict[str, Any]] = None,
        metadata: Optional[Dict[str, Any]] = None,
    ) -> None:
        """
        Save a checkpoint with current pipeline state

        Parameters
        step : str
            Current pipeline step (e.g., 'extract', 'transform', 'load')
        data : Dict[str, Any], optional
            Data to save with the checkpoint
        metadata : Dict[str, Any], optional
            Additional metadata to save
        """
        checkpoint_data = {
            "step": step,
            "timestamp": pd.Timestamp.now().isoformat(),
            "metadata": metadata or {},
        }

        # Save checkpoint metadata
        with open(self.checkpoint_file, "w", encoding="utf-8") as f:
            json.dump(checkpoint_data, f, indent=2, default=str)

        # Save data state if provided
        if data:
            with open(self.state_file, "wb") as f:
                pickle.dump(data, f)

        LOGGER.info("Checkpoint saved at step: %s", step)

    def load_checkpoint(self) -> Tuple[Optional[str], Optional[Dict[str, Any]]]:
        """
        Load the last checkpoint

        Returns
        Tuple[Optional[str], Optional[Dict[str, Any]]]
            (step, data) - step to resume from and saved data, or (None, None) if no checkpoint
        """
        if not self.checkpoint_file.exists():
            return None, None

        try:
            # Load checkpoint metadata
            with open(self.checkpoint_file, "r", encoding="utf-8") as f:
                checkpoint_data = json.load(f)

            step = checkpoint_data["step"]

            # Load data state if it exists
            data = None
            if self.state_file.exists():
                with open(self.state_file, "rb") as f:
                    data = pickle.load(f)

            LOGGER.info("Checkpoint loaded - resuming from step: %s", step)
            return step, data

        except (json.JSONDecodeError, pickle.UnpicklingError, KeyError) as e:
            LOGGER.warning("Failed to load checkpoint: %s", e)
            return None, None

    def clear_checkpoint(self) -> None:
        """Clear all checkpoint files."""
        if self.checkpoint_file.exists():
            self.checkpoint_file.unlink()
        if self.state_file.exists():
            self.state_file.unlink()
        LOGGER.info("Checkpoints cleared")

    def get_checkpoint_info(self) -> Optional[Dict[str, Any]]:
        """Get information about the current checkpoint."""
        if not self.checkpoint_file.exists():
            return None

        try:
            with open(self.checkpoint_file, "r", encoding="utf-8") as f:
                return json.load(f)
        except json.JSONDecodeError:
            return None


def should_resume_from_checkpoint(checkpoint: PipelineCheckpoint) -> bool:
    """
    Determine if the pipeline should resume from a checkpoint.

    Parameters
    checkpoint : PipelineCheckpoint
        The checkpoint manager

    Returns
    bool
        True if should resume, False otherwise
    """
    step, _ = checkpoint.load_checkpoint()
    return step is not None


def resume_pipeline_from_checkpoint(
    checkpoint: PipelineCheckpoint,
    config: Dict[str, Any],
) -> Optional[Dict[str, Any]]:
    """
    Resume pipeline execution from the last checkpoint.

    Parameters
    checkpoint : PipelineCheckpoint
        The checkpoint manager
    config : Dict[str, Any]
        Pipeline configuration

    Returns
    Dict[str, Any] or None
        Resumed data state, or None if cannot resume
    """
    step, data = checkpoint.load_checkpoint()

    if step is None:
        LOGGER.info("No checkpoint found - starting fresh pipeline run")
        return None

    LOGGER.info("Resuming pipeline from checkpoint: %s", step)

    # Based on the step, we would need to reload data and continue
    # For now, return the saved data - the main pipeline logic will handle resumption
    return data