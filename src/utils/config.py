"""Configuration loader utility."""

from pathlib import Path
from typing import Any, Dict

import yaml


def load_config(config_path: str = "configs/config.yaml") -> Dict[str, Any]:
    """
    Load configuration from YAML file.

    Args:
        config_path: Path to the configuration file

    Returns:
        Dictionary containing configuration parameters
    """
    config_file = Path(config_path)

    if not config_file.exists():
        raise FileNotFoundError(f"Configuration file not found: {config_path}")

    with open(config_file, "r", encoding="utf-8") as f:
        config = yaml.safe_load(f)

    return config


def get_insee_urls(config: Dict[str, Any] = None) -> Dict[str, str]:
    """
    Extract INSEE download URLs from configuration.

    Args:
        config: Configuration dictionary (if None, will load from default path)

    Returns:
        Dictionary with INSEE file download URLs
    """
    if config is None:
        config = load_config()

    insee_config = config.get("insee", {})

    # Return only downloadable URLs
    return insee_config.get("download_urls", {})


def get_insee_metadata(config: Dict[str, Any] = None) -> Dict[str, str]:
    """
    Extract INSEE metadata from configuration.

    Args:
        config: Configuration dictionary (if None, will load from default path)

    Returns:
        Dictionary with INSEE metadata (portal, documentation, etc.)
    """
    if config is None:
        config = load_config()

    insee_config = config.get("insee", {})

    return insee_config.get("metadata", {})


def get_data_paths(config: Dict[str, Any] = None) -> Dict[str, str]:
    """
    Extract data paths from configuration.

    Args:
        config: Configuration dictionary (if None, will load from default path)

    Returns:
        Dictionary with data paths
    """
    if config is None:
        config = load_config()

    return config.get("paths", {})
