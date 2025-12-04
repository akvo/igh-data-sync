"""Tests for type mapping and data structures."""

from lib.type_mapping import (
    ColumnMetadata,
    ForeignKeyMetadata,
    map_edm_to_db_type,
    normalize_db_type,
)


class TestTypeMappingSQLite:
    """Test Edm to SQLite type mapping."""

    def test_basic_type_mapping(self):
        """Test basic Edm type to SQLite mapping."""
        assert map_edm_to_db_type("Edm.String", "sqlite") == "TEXT"
        assert map_edm_to_db_type("Edm.Int32", "sqlite") == "INTEGER"
        assert map_edm_to_db_type("Edm.Decimal", "sqlite") == "REAL"
        assert map_edm_to_db_type("Edm.Boolean", "sqlite") == "INTEGER"
        assert map_edm_to_db_type("Edm.Guid", "sqlite") == "TEXT"

    def test_unknown_type_defaults_to_text(self):
        """Test that unknown types default to TEXT."""
        assert map_edm_to_db_type("Edm.Unknown", "sqlite") == "TEXT"


class TestTypeMappingPostgreSQL:
    """Test Edm to PostgreSQL type mapping."""

    def test_basic_type_mapping(self):
        """Test basic Edm type to PostgreSQL mapping."""
        assert map_edm_to_db_type("Edm.String", "postgresql") == "TEXT"
        assert map_edm_to_db_type("Edm.Int32", "postgresql") == "INTEGER"
        assert map_edm_to_db_type("Edm.Decimal", "postgresql") == "NUMERIC"
        assert map_edm_to_db_type("Edm.Boolean", "postgresql") == "BOOLEAN"
        assert map_edm_to_db_type("Edm.Guid", "postgresql") == "UUID"

    def test_varchar_with_max_length(self):
        """Test VARCHAR type with max_length specified."""
        assert map_edm_to_db_type("Edm.String", "postgresql", max_length=100) == "VARCHAR(100)"

    def test_varchar_without_max_length(self):
        """Test VARCHAR type without max_length defaults to TEXT."""
        assert map_edm_to_db_type("Edm.String", "postgresql", max_length=None) == "TEXT"


class TestTypeNormalization:
    """Test database type normalization for comparison."""

    def test_sqlite_normalization(self):
        """Test SQLite type normalization."""
        assert normalize_db_type("VARCHAR", "sqlite") == "TEXT"
        assert normalize_db_type("TEXT", "sqlite") == "TEXT"
        assert normalize_db_type("INT", "sqlite") == "INTEGER"
        assert normalize_db_type("INTEGER", "sqlite") == "INTEGER"
        assert normalize_db_type("DOUBLE", "sqlite") == "REAL"
        assert normalize_db_type("FLOAT", "sqlite") == "REAL"

    def test_postgresql_normalization(self):
        """Test PostgreSQL type normalization."""
        assert normalize_db_type("VARCHAR", "postgresql") == "TEXT"
        assert normalize_db_type("CHARACTER VARYING", "postgresql") == "TEXT"
        assert normalize_db_type("INT", "postgresql") == "INTEGER"
        assert normalize_db_type("INT4", "postgresql") == "INTEGER"
        assert normalize_db_type("BOOL", "postgresql") == "BOOLEAN"

    def test_length_specification_removed(self):
        """Test that length specifications are removed for comparison."""
        assert normalize_db_type("VARCHAR(100)", "sqlite") == "TEXT"
        assert normalize_db_type("NUMERIC(10,2)", "postgresql") == "NUMERIC"


class TestColumnMetadata:
    """Test ColumnMetadata equality."""

    def test_column_equality(self):
        """Test that columns are equal when all fields match."""
        col1 = ColumnMetadata("name", "TEXT", nullable=True)
        col2 = ColumnMetadata("name", "TEXT", nullable=True)
        assert col1 == col2

    def test_column_equality_case_insensitive(self):
        """Test that column comparison is case-insensitive."""
        col1 = ColumnMetadata("name", "TEXT", nullable=True)
        col2 = ColumnMetadata("NAME", "text", nullable=True)
        assert col1 == col2

    def test_column_inequality_different_nullable(self):
        """Test that columns are not equal if nullable differs."""
        col1 = ColumnMetadata("name", "TEXT", nullable=True)
        col2 = ColumnMetadata("name", "TEXT", nullable=False)
        assert col1 != col2


class TestForeignKeyMetadata:
    """Test ForeignKeyMetadata equality."""

    def test_fk_equality(self):
        """Test that foreign keys are equal when all fields match."""
        fk1 = ForeignKeyMetadata("col", "table", "id")
        fk2 = ForeignKeyMetadata("col", "table", "id")
        assert fk1 == fk2

    def test_fk_equality_case_insensitive(self):
        """Test that foreign key comparison is case-insensitive."""
        fk1 = ForeignKeyMetadata("col", "table", "id")
        fk2 = ForeignKeyMetadata("COL", "TABLE", "ID")
        assert fk1 == fk2
