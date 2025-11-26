"""Tests for configuration loading with entity name mapping."""
import unittest
import tempfile
import json
import os
from lib.config import load_entity_configs, EntityConfig


class TestEntityConfig(unittest.TestCase):
    """Test entity configuration loading with name mapping."""

    def test_auto_pluralization(self):
        """Test automatic pluralization of entity names."""
        config_data = {
            "entities": [
                {"name": "account", "filtered": False, "description": "Test"}
            ]
        }

        with tempfile.NamedTemporaryFile(mode='w', delete=False, suffix='.json') as f:
            json.dump(config_data, f)
            config_path = f.name

        try:
            entities = load_entity_configs(config_path)
            self.assertEqual(len(entities), 1)
            self.assertEqual(entities[0].name, "account")
            self.assertEqual(entities[0].api_name, "accounts")  # Auto-pluralized
        finally:
            os.unlink(config_path)

    def test_explicit_api_name(self):
        """Test explicit api_name overrides pluralization."""
        config_data = {
            "entities": [
                {
                    "name": "vin_candidate",
                    "api_name": "vin_candidates",
                    "filtered": False,
                    "description": "Test"
                }
            ]
        }

        with tempfile.NamedTemporaryFile(mode='w', delete=False, suffix='.json') as f:
            json.dump(config_data, f)
            config_path = f.name

        try:
            entities = load_entity_configs(config_path)
            self.assertEqual(len(entities), 1)
            self.assertEqual(entities[0].name, "vin_candidate")
            self.assertEqual(entities[0].api_name, "vin_candidates")
        finally:
            os.unlink(config_path)

    def test_multiple_entities(self):
        """Test loading multiple entities with mixed config."""
        config_data = {
            "entities": [
                {"name": "account", "filtered": True, "description": "Filtered"},
                {"name": "vin_candidate", "api_name": "vin_candidates", "filtered": False, "description": "Explicit"}
            ]
        }

        with tempfile.NamedTemporaryFile(mode='w', delete=False, suffix='.json') as f:
            json.dump(config_data, f)
            config_path = f.name

        try:
            entities = load_entity_configs(config_path)
            self.assertEqual(len(entities), 2)

            # Check first entity (auto-pluralized)
            self.assertEqual(entities[0].name, "account")
            self.assertEqual(entities[0].api_name, "accounts")
            self.assertTrue(entities[0].filtered)

            # Check second entity (explicit)
            self.assertEqual(entities[1].name, "vin_candidate")
            self.assertEqual(entities[1].api_name, "vin_candidates")
            self.assertFalse(entities[1].filtered)
        finally:
            os.unlink(config_path)


if __name__ == '__main__':
    unittest.main()
