"""Configuration loading for Dataverse schema validator."""

import json
import os
from dataclasses import dataclass
from pathlib import Path

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


def load_config(env_path: str = ".env") -> Config:
    """
    Load configuration from .env file.

    Args:
        env_path: Path to .env file (default: '.env')

    Returns:
        Config object with loaded settings

    Raises:
        ValueError: If required configuration is missing
    """
    load_dotenv(env_path)

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


def load_entities(path: str = "entities_config.json") -> list[str]:
    """
    Load entity names from entities_config.json.

    Args:
        path: Path to entities configuration file

    Returns:
        List of entity names (logical names, singular form)

    Raises:
        FileNotFoundError: If config file doesn't exist
        ValueError: If config file is invalid
    """
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


def load_entity_configs(path: str = "entities_config.json") -> list[EntityConfig]:
    """
    Load full entity configurations from entities_config.json.

    Auto-pluralizes entity names for API endpoints if api_name not specified.
    Pluralization: simply adds 's' to the end (e.g., vin_candidate → vin_candidates)

    Args:
        path: Path to entities configuration file

    Returns:
        List of EntityConfig objects with both name (singular) and api_name (plural)

    Raises:
        FileNotFoundError: If config file doesn't exist
        ValueError: If config file is invalid
    """
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
