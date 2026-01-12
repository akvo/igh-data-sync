"""Tests for configuration loading with entity name mapping."""

import json

from igh_data_sync.config import load_entity_configs


class TestEntityConfig:
    """Test entity configuration loading with name mapping."""

    def test_auto_pluralization(self, tmp_path):
        """Test automatic pluralization of entity names."""
        config_data = {"entities": [{"name": "account", "filtered": False, "description": "Test"}]}

        config_path = tmp_path / "config.json"
        config_path.write_text(json.dumps(config_data))

        entities = load_entity_configs(str(config_path))
        assert len(entities) == 1
        assert entities[0].name == "account"
        assert entities[0].api_name == "accounts"  # Auto-pluralized

    def test_explicit_api_name(self, tmp_path):
        """Test explicit api_name overrides pluralization."""
        config_data = {
            "entities": [
                {
                    "name": "vin_candidate",
                    "api_name": "vin_candidates",
                    "filtered": False,
                    "description": "Test",
                },
            ],
        }

        config_path = tmp_path / "config.json"
        config_path.write_text(json.dumps(config_data))

        entities = load_entity_configs(str(config_path))
        assert len(entities) == 1
        assert entities[0].name == "vin_candidate"
        assert entities[0].api_name == "vin_candidates"

    def test_multiple_entities(self, tmp_path):
        """Test loading multiple entities with mixed config."""
        config_data = {
            "entities": [
                {"name": "account", "filtered": True, "description": "Filtered"},
                {
                    "name": "vin_candidate",
                    "api_name": "vin_candidates",
                    "filtered": False,
                    "description": "Explicit",
                },
            ],
        }

        config_path = tmp_path / "config.json"
        config_path.write_text(json.dumps(config_data))

        entities = load_entity_configs(str(config_path))
        assert len(entities) == 2

        # Check first entity (auto-pluralized)
        assert entities[0].name == "account"
        assert entities[0].api_name == "accounts"
        assert entities[0].filtered is True

        # Check second entity (explicit)
        assert entities[1].name == "vin_candidate"
        assert entities[1].api_name == "vin_candidates"
        assert entities[1].filtered is False
