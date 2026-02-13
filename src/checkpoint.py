"""Checkpoint and resume functionality for the ETL pipeline

This module provides utilities to save pipeline state at checkpoints and
resume execution from the last successful checkpoint in case of failures.

Security note: DataFrames are serialized to Parquet (not pickle) to prevent
code execution vulnerabilities. Non-DataFrame data uses JSON serialization.
"""

from __future__ import annotations

import hashlib
import json
import logging
from pathlib import Path
from typing import Dict, Any, Optional, Tuple
import pandas as pd

# Logging
LOGGER = logging.getLogger(__name__)


def compute_config_hash(config: Dict[str, Any]) -> str:
    """
    Compute a stable hash of the configuration dictionary.

    Parameters
    ----------
    config : Dict[str, Any]
        Pipeline configuration dictionary

    Returns
    -------
    str
        SHA-256 hash of the serialized configuration
    """
    # Serialize config to JSON with sorted keys for consistency
    config_json = json.dumps(config, sort_keys=True, separators=(',', ':'), default=str)
    return hashlib.sha256(config_json.encode('utf-8')).hexdigest()


class ConfigMismatchError(Exception):
    """Raised when checkpoint configuration doesn't match current configuration."""

    def __init__(self, message: str = "Configuration mismatch detected") -> None:
        self.message = message
        super().__init__(self.message)


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
        self.state_dir = self.checkpoint_dir / "state"
        self.state_dir.mkdir(parents=True, exist_ok=True)
        self.state_manifest = self.state_dir / "manifest.json"

    def save_checkpoint(
        self,
        step: str,
        data: Optional[Dict[str, Any]] = None,
        metadata: Optional[Dict[str, Any]] = None,
        config: Optional[Dict[str, Any]] = None,
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
        config : Dict[str, Any], optional
            Pipeline configuration to hash and store for validation on resume
        """
        checkpoint_data: Dict[str, Any] = {
            "step": step,
            "timestamp": pd.Timestamp.now().isoformat(),
            "metadata": metadata or {},
        }

        # Store config hash if provided
        if config is not None:
            checkpoint_data["config_hash"] = compute_config_hash(config)

        # Save checkpoint metadata
        with open(self.checkpoint_file, "w", encoding="utf-8") as f:
            json.dump(checkpoint_data, f, indent=2, default=str)

        # Save data state if provided (using Parquet for DataFrames, JSON for other data)
        if data:
            manifest = self.save_state_data(data)
            with open(self.state_manifest, "w", encoding="utf-8") as f:
                json.dump(manifest, f, indent=2, default=str)

        LOGGER.info("Checkpoint saved at step: %s", step)

    def save_state_data(self, data: Dict[str, Any]) -> Dict[str, Any]:
        """
        Save checkpoint state data securely using Parquet for DataFrames.

        Parameters
        ----------
        data : Dict[str, Any]
            Data to save - DataFrames are saved as Parquet, other types as JSON

        Returns
        -------
        Dict[str, Any]
            Manifest describing saved data
        """
        manifest = {"dataframes": {}, "metadata": {}}

        for key, value in data.items():
            if isinstance(value, pd.DataFrame):
                # Save DataFrame as Parquet (secure, cross-platform)
                df_path = self.state_dir / f"{key}.parquet"
                value.to_parquet(df_path, index=False)
                manifest["dataframes"][key] = str(df_path.relative_to(self.checkpoint_dir))
            else:
                # Save non-DataFrame data as JSON
                metadata_path = self.state_dir / f"{key}.json"
                with open(metadata_path, "w", encoding="utf-8") as f:
                    json.dump(value, f, indent=2, default=str)
                manifest["metadata"][key] = str(metadata_path.relative_to(self.checkpoint_dir))

        return manifest

    def load_state_data(self, manifest: Dict[str, Any]) -> Dict[str, Any]:
        """
        Load checkpoint state data from manifest.

        Parameters
        ----------
        manifest : Dict[str, Any]
            Manifest describing saved data

        Returns
        -------
        Dict[str, Any]
            Restored data with DataFrames loaded from Parquet
        """
        data: Dict[str, Any] = {}

        # Load DataFrames from Parquet
        for key, relative_path in manifest.get("dataframes", {}).items():
            df_path = self.checkpoint_dir / relative_path
            if df_path.exists():
                data[key] = pd.read_parquet(df_path)
            else:
                LOGGER.warning("DataFrame file not found: %s", df_path)

        # Load metadata from JSON
        for key, relative_path in manifest.get("metadata", {}).items():
            metadata_path = self.checkpoint_dir / relative_path
            if metadata_path.exists():
                with open(metadata_path, "r", encoding="utf-8") as f:
                    data[key] = json.load(f)
            else:
                LOGGER.warning("Metadata file not found: %s", metadata_path)

        return data

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

            # Load data state if manifest exists
            data = None
            if self.state_manifest.exists():
                with open(self.state_manifest, "r", encoding="utf-8") as f:
                    manifest = json.load(f)
                data = self.load_state_data(manifest)

            LOGGER.info("Checkpoint loaded - resuming from step: %s", step)
            return step, data

        except (json.JSONDecodeError, KeyError, FileNotFoundError) as e:
            LOGGER.warning("Failed to load checkpoint: %s", e)
            return None, None

    def clear_checkpoint(self) -> None:
        """Clear all checkpoint files."""
        if self.checkpoint_file.exists():
            self.checkpoint_file.unlink()
        if self.state_manifest.exists():
            self.state_manifest.unlink()
        # Clean up all files in state directory
        if self.state_dir.exists():
            for state_file in self.state_dir.iterdir():
                if state_file.is_file():
                    state_file.unlink()
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