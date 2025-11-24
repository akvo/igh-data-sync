"""Configuration loading for Dataverse schema validator."""
import os
import json
from dataclasses import dataclass
from typing import List, Dict
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
            return 'postgresql'
        elif self.sqlite_db_path:
            return 'sqlite'
        else:
            raise ValueError("No database configured. Set either SQLITE_DB_PATH or POSTGRES_CONNECTION_STRING")


@dataclass
class EntityConfig:
    """Configuration for a single entity."""
    name: str
    filtered: bool
    description: str


def load_config(env_path: str = '.env') -> Config:
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
    api_url = os.getenv('DATAVERSE_API_URL')
    client_id = os.getenv('DATAVERSE_CLIENT_ID')
    client_secret = os.getenv('DATAVERSE_CLIENT_SECRET')
    scope = os.getenv('DATAVERSE_SCOPE')

    # Validate required fields
    if not all([api_url, client_id, client_secret, scope]):
        missing = []
        if not api_url:
            missing.append('DATAVERSE_API_URL')
        if not client_id:
            missing.append('DATAVERSE_CLIENT_ID')
        if not client_secret:
            missing.append('DATAVERSE_CLIENT_SECRET')
        if not scope:
            missing.append('DATAVERSE_SCOPE')
        raise ValueError(f"Missing required environment variables: {', '.join(missing)}")

    # Optional database configuration
    sqlite_db_path = os.getenv('SQLITE_DB_PATH')
    postgres_connection_string = os.getenv('POSTGRES_CONNECTION_STRING')

    return Config(
        api_url=api_url.rstrip('/'),
        client_id=client_id,
        client_secret=client_secret,
        scope=scope,
        sqlite_db_path=sqlite_db_path,
        postgres_connection_string=postgres_connection_string
    )


def load_entities(path: str = 'entities_config.json') -> List[str]:
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
        raise FileNotFoundError(f"Entity configuration file not found: {path}")

    with open(config_path, 'r') as f:
        config = json.load(f)

    if 'entities' not in config:
        raise ValueError("Invalid entities_config.json: missing 'entities' key")

    entities = config['entities']
    if not isinstance(entities, list):
        raise ValueError("Invalid entities_config.json: 'entities' must be a list")

    entity_names = []
    for entity in entities:
        if not isinstance(entity, dict) or 'name' not in entity:
            raise ValueError(f"Invalid entity entry: {entity}")
        entity_names.append(entity['name'])

    return entity_names


def load_entity_configs(path: str = 'entities_config.json') -> List[EntityConfig]:
    """
    Load full entity configurations from entities_config.json.

    Args:
        path: Path to entities configuration file

    Returns:
        List of EntityConfig objects

    Raises:
        FileNotFoundError: If config file doesn't exist
        ValueError: If config file is invalid
    """
    config_path = Path(path)

    if not config_path.exists():
        raise FileNotFoundError(f"Entity configuration file not found: {path}")

    with open(config_path, 'r') as f:
        config = json.load(f)

    if 'entities' not in config:
        raise ValueError("Invalid entities_config.json: missing 'entities' key")

    entities = config['entities']
    if not isinstance(entities, list):
        raise ValueError("Invalid entities_config.json: 'entities' must be a list")

    entity_configs = []
    for entity in entities:
        if not isinstance(entity, dict):
            raise ValueError(f"Invalid entity entry: {entity}")

        entity_configs.append(EntityConfig(
            name=entity.get('name', ''),
            filtered=entity.get('filtered', False),
            description=entity.get('description', '')
        ))

    return entity_configs
