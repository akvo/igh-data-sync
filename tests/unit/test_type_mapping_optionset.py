"""Tests for option set type mapping override."""

from lib.type_mapping import map_edm_to_db_type


def test_option_set_overrides_edm_string_to_integer():
    """Option sets should map Edm.String → INTEGER when is_option_set=True."""
    result = map_edm_to_db_type("Edm.String", "sqlite", is_option_set=True)
    assert result == "INTEGER"


def test_regular_string_maps_to_text():
    """Regular strings should still map to TEXT when is_option_set=False."""
    result = map_edm_to_db_type("Edm.String", "sqlite", is_option_set=False)
    assert result == "TEXT"


def test_option_set_default_parameter():
    """When is_option_set is not specified, default should be False (TEXT)."""
    result = map_edm_to_db_type("Edm.String", "sqlite")
    assert result == "TEXT"


def test_option_set_does_not_affect_other_types():
    """is_option_set flag should only affect Edm.String, not other types."""
    # Edm.Int32 should always be INTEGER regardless of is_option_set
    result1 = map_edm_to_db_type("Edm.Int32", "sqlite", is_option_set=True)
    result2 = map_edm_to_db_type("Edm.Int32", "sqlite", is_option_set=False)
    assert result1 == "INTEGER"
    assert result2 == "INTEGER"

    # Edm.Boolean should always be INTEGER (SQLite) regardless of is_option_set
    result3 = map_edm_to_db_type("Edm.Boolean", "sqlite", is_option_set=True)
    result4 = map_edm_to_db_type("Edm.Boolean", "sqlite", is_option_set=False)
    assert result3 == "INTEGER"
    assert result4 == "INTEGER"


def test_option_set_with_max_length():
    """Option sets with max_length specified should still return INTEGER."""
    result = map_edm_to_db_type("Edm.String", "sqlite", max_length=100, is_option_set=True)
    assert result == "INTEGER"


def test_option_set_postgresql():
    """Option sets should map to INTEGER for PostgreSQL as well."""
    result = map_edm_to_db_type("Edm.String", "postgresql", is_option_set=True)
    assert result == "INTEGER"


def test_regular_string_postgresql_with_length():
    """Regular strings in PostgreSQL with max_length should map to VARCHAR(n)."""
    result = map_edm_to_db_type("Edm.String", "postgresql", max_length=255, is_option_set=False)
    assert result == "VARCHAR(255)"


def test_option_set_postgresql_with_length_still_integer():
    """Option sets in PostgreSQL with max_length should still be INTEGER, not VARCHAR."""
    result = map_edm_to_db_type("Edm.String", "postgresql", max_length=255, is_option_set=True)
    assert result == "INTEGER"
