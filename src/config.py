import yaml
from pathlib import Path
from typing import Union
from src.config_schema import load_and_validate_config, PipelineConfig


def load_config(config_path: str = "pipeline_config.yaml") -> dict:
    """
    Load configuration from YAML file with validation.

    Parameters
    ----------
    config_path : str
        Path to the configuration YAML file (relative to project root or absolute)

    Returns
    -------
    dict
        Validated configuration dictionary

    Raises
    ------
    FileNotFoundError
        If config file doesn't exist
    ValidationError
        If configuration is invalid
    """
    # Use the validated config loader
    validated_config = load_and_validate_config(config_path)

    # Return as dict for backward compatibility
    return validated_config.model_dump()


def load_config_validated(config_path: str = "pipeline_config.yaml") -> PipelineConfig:
    """
    Load and return validated configuration object.

    Parameters
    ----------
    config_path : str
        Path to the configuration YAML file

    Returns
    -------
    PipelineConfig
        Validated configuration object with type hints and validation
    """
    return load_and_validate_config(config_path)
