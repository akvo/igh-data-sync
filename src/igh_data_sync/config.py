"""Configuration loading for Dataverse schema validator."""

import json
import os
from dataclasses import dataclass
from pathlib import Path
from typing import Optional

try:
    from importlib.resources import files
except ImportError:
    # Python <3.9 fallback
    from importlib_resources import files  # type: ignore

from dotenv import load_dotenv


@dataclass
class Config:
    """Configuration for Dataverse API and database access."""

    api_url: str
    client_id: str
    client_secret: str
    scope: str
    sqlite_db_path: str = None
    postgres_connection_string: str = None

    def get_db_type(self) -> str:
        """Determine which database type is configured."""
        if self.postgres_connection_string:
            return "postgresql"
        elif self.sqlite_db_path:
            return "sqlite"
        else:
            msg = "No database configured. Set either SQLITE_DB_PATH or POSTGRES_CONNECTION_STRING"
            raise ValueError(
                msg,
            )


@dataclass
class EntityConfig:
    """Configuration for a single entity."""

    name: str  # Singular name from $metadata (e.g., vin_candidate)
    api_name: str  # Plural name for API endpoint (e.g., vin_candidates)
    filtered: bool
    description: str


def get_default_config_path(filename: str) -> str:
    """
    Get default config file path from package data.

    Args:
        filename: Name of the config file (e.g., 'entities_config.json')

    Returns:
        Absolute path to the config file in package data directory
    """
    return str(files("igh_data_sync").joinpath(f"data/{filename}"))


def load_config(env_file: Optional[str] = None) -> Config:
    """
    Load configuration from environment variables.

    Environment variable loading precedence:
    1. If env_file provided via CLI, load from that path
    2. Otherwise, check for .env in current working directory
    3. Otherwise, use system environment variables

    Args:
        env_file: Optional path to .env file (CLI parameter)

    Returns:
        Config object with loaded settings

    Raises:
        ValueError: If required configuration is missing
    """
    # Load environment variables with proper precedence
    if env_file:
        # Explicit path provided via CLI
        load_dotenv(env_file)
    elif Path(".env").exists():
        # .env in working directory
        load_dotenv(".env")
    # Otherwise, use system environment variables (no action needed)

    # Required fields
    api_url = os.getenv("DATAVERSE_API_URL")
    client_id = os.getenv("DATAVERSE_CLIENT_ID")
    client_secret = os.getenv("DATAVERSE_CLIENT_SECRET")
    scope = os.getenv("DATAVERSE_SCOPE")

    # Validate required fields
    if not all([api_url, client_id, client_secret, scope]):
        missing = []
        if not api_url:
            missing.append("DATAVERSE_API_URL")
        if not client_id:
            missing.append("DATAVERSE_CLIENT_ID")
        if not client_secret:
            missing.append("DATAVERSE_CLIENT_SECRET")
        if not scope:
            missing.append("DATAVERSE_SCOPE")
        msg = f"Missing required environment variables: {', '.join(missing)}"
        raise ValueError(msg)

    # Optional database configuration
    sqlite_db_path = os.getenv("SQLITE_DB_PATH")
    postgres_connection_string = os.getenv("POSTGRES_CONNECTION_STRING")

    return Config(
        api_url=api_url.rstrip("/"),
        client_id=client_id,
        client_secret=client_secret,
        scope=scope,
        sqlite_db_path=sqlite_db_path,
        postgres_connection_string=postgres_connection_string,
    )


def load_entities(path: Optional[str] = None) -> list[str]:
    """
    Load entity names from entities_config.json.

    Args:
        path: Optional path to entities configuration file.
              If None, uses package default from data/entities_config.json

    Returns:
        List of entity names (logical names, singular form)

    Raises:
        FileNotFoundError: If config file doesn't exist
        ValueError: If config file is invalid
    """
    # Use package default if no path provided
    if path is None:
        path = get_default_config_path("entities_config.json")

    config_path = Path(path)

    if not config_path.exists():
        msg = f"Entity configuration file not found: {path}"
        raise FileNotFoundError(msg)

    with Path(config_path).open(encoding="utf-8") as f:
        config = json.load(f)

    if "entities" not in config:
        msg = "Invalid entities_config.json: missing 'entities' key"
        raise ValueError(msg)

    entities = config["entities"]
    if not isinstance(entities, list):
        msg = "Invalid entities_config.json: 'entities' must be a list"
        raise TypeError(msg)

    entity_names = []
    for entity in entities:
        if not isinstance(entity, dict) or "name" not in entity:
            msg = f"Invalid entity entry: {entity}"
            raise ValueError(msg)
        entity_names.append(entity["name"])

    return entity_names


def load_entity_configs(path: Optional[str] = None) -> list[EntityConfig]:
    """
    Load full entity configurations from entities_config.json.

    Auto-pluralizes entity names for API endpoints if api_name not specified.
    Pluralization: simply adds 's' to the end (e.g., vin_candidate → vin_candidates)

    Args:
        path: Optional path to entities configuration file.
              If None, uses package default from data/entities_config.json

    Returns:
        List of EntityConfig objects with both name (singular) and api_name (plural)

    Raises:
        FileNotFoundError: If config file doesn't exist
        ValueError: If config file is invalid
    """
    # Use package default if no path provided
    if path is None:
        path = get_default_config_path("entities_config.json")

    config_path = Path(path)

    if not config_path.exists():
        msg = f"Entity configuration file not found: {path}"
        raise FileNotFoundError(msg)

    with Path(config_path).open(encoding="utf-8") as f:
        config = json.load(f)

    if "entities" not in config:
        msg = "Invalid entities_config.json: missing 'entities' key"
        raise ValueError(msg)

    entities = config["entities"]
    if not isinstance(entities, list):
        msg = "Invalid entities_config.json: 'entities' must be a list"
        raise TypeError(msg)

    entity_configs = []
    for entity in entities:
        if not isinstance(entity, dict):
            msg = f"Invalid entity entry: {entity}"
            raise TypeError(msg)

        name = entity.get("name", "")

        # Auto-pluralize if api_name not specified
        # Simple rule: add 's' to the end
        api_name = entity.get("api_name", name + "s" if name else "")

        entity_configs.append(
            EntityConfig(
                name=name,
                api_name=api_name,
                filtered=entity.get("filtered", False),
                description=entity.get("description", ""),
            ),
        )

    return entity_configs


def load_optionsets_config(path: Optional[str] = None) -> dict:
    """
    Load option set configuration from optionsets.json.

    Args:
        path: Optional path to optionsets configuration file.
              If None, uses package default from data/optionsets.json

    Returns:
        Dict mapping entity names to lists of option set field names

    Raises:
        FileNotFoundError: If config file doesn't exist
        ValueError: If config file is invalid
    """
    # Use package default if no path provided
    if path is None:
        path = get_default_config_path("optionsets.json")

    config_path = Path(path)

    if not config_path.exists():
        msg = f"Option sets configuration file not found: {path}"
        raise FileNotFoundError(msg)

    with Path(config_path).open(encoding="utf-8") as f:
        config = json.load(f)

    if not isinstance(config, dict):
        msg = "Invalid optionsets.json: must be a dictionary"
        raise TypeError(msg)

    return config
