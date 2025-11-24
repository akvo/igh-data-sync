"""Tests for type mapping and data structures."""
import unittest
from lib.type_mapping import (
    ColumnMetadata, ForeignKeyMetadata, TableSchema,
    map_edm_to_db_type, normalize_db_type,
    EDM_TYPE_MAP_SQLITE, EDM_TYPE_MAP_POSTGRESQL
)


class TestTypeMappingSQLite(unittest.TestCase):
    """Test Edm to SQLite type mapping."""

    def test_basic_type_mapping(self):
        """Test basic Edm type to SQLite mapping."""
        self.assertEqual(map_edm_to_db_type('Edm.String', 'sqlite'), 'TEXT')
        self.assertEqual(map_edm_to_db_type('Edm.Int32', 'sqlite'), 'INTEGER')
        self.assertEqual(map_edm_to_db_type('Edm.Decimal', 'sqlite'), 'REAL')
        self.assertEqual(map_edm_to_db_type('Edm.Boolean', 'sqlite'), 'INTEGER')
        self.assertEqual(map_edm_to_db_type('Edm.Guid', 'sqlite'), 'TEXT')

    def test_unknown_type_defaults_to_text(self):
        """Test that unknown types default to TEXT."""
        self.assertEqual(map_edm_to_db_type('Edm.Unknown', 'sqlite'), 'TEXT')


class TestTypeMappingPostgreSQL(unittest.TestCase):
    """Test Edm to PostgreSQL type mapping."""

    def test_basic_type_mapping(self):
        """Test basic Edm type to PostgreSQL mapping."""
        self.assertEqual(map_edm_to_db_type('Edm.String', 'postgresql'), 'TEXT')
        self.assertEqual(map_edm_to_db_type('Edm.Int32', 'postgresql'), 'INTEGER')
        self.assertEqual(map_edm_to_db_type('Edm.Decimal', 'postgresql'), 'NUMERIC')
        self.assertEqual(map_edm_to_db_type('Edm.Boolean', 'postgresql'), 'BOOLEAN')
        self.assertEqual(map_edm_to_db_type('Edm.Guid', 'postgresql'), 'UUID')

    def test_varchar_with_max_length(self):
        """Test VARCHAR type with max_length specified."""
        self.assertEqual(
            map_edm_to_db_type('Edm.String', 'postgresql', max_length=100),
            'VARCHAR(100)'
        )

    def test_varchar_without_max_length(self):
        """Test VARCHAR type without max_length defaults to TEXT."""
        self.assertEqual(
            map_edm_to_db_type('Edm.String', 'postgresql', max_length=None),
            'TEXT'
        )


class TestTypeNormalization(unittest.TestCase):
    """Test database type normalization for comparison."""

    def test_sqlite_normalization(self):
        """Test SQLite type normalization."""
        self.assertEqual(normalize_db_type('VARCHAR', 'sqlite'), 'TEXT')
        self.assertEqual(normalize_db_type('TEXT', 'sqlite'), 'TEXT')
        self.assertEqual(normalize_db_type('INT', 'sqlite'), 'INTEGER')
        self.assertEqual(normalize_db_type('INTEGER', 'sqlite'), 'INTEGER')
        self.assertEqual(normalize_db_type('DOUBLE', 'sqlite'), 'REAL')
        self.assertEqual(normalize_db_type('FLOAT', 'sqlite'), 'REAL')

    def test_postgresql_normalization(self):
        """Test PostgreSQL type normalization."""
        self.assertEqual(normalize_db_type('VARCHAR', 'postgresql'), 'TEXT')
        self.assertEqual(normalize_db_type('CHARACTER VARYING', 'postgresql'), 'TEXT')
        self.assertEqual(normalize_db_type('INT', 'postgresql'), 'INTEGER')
        self.assertEqual(normalize_db_type('INT4', 'postgresql'), 'INTEGER')
        self.assertEqual(normalize_db_type('BOOL', 'postgresql'), 'BOOLEAN')

    def test_length_specification_removed(self):
        """Test that length specifications are removed for comparison."""
        self.assertEqual(normalize_db_type('VARCHAR(100)', 'sqlite'), 'TEXT')
        self.assertEqual(normalize_db_type('NUMERIC(10,2)', 'postgresql'), 'NUMERIC')


class TestColumnMetadata(unittest.TestCase):
    """Test ColumnMetadata equality."""

    def test_column_equality(self):
        """Test that columns are equal when all fields match."""
        col1 = ColumnMetadata('name', 'TEXT', nullable=True)
        col2 = ColumnMetadata('name', 'TEXT', nullable=True)
        self.assertEqual(col1, col2)

    def test_column_equality_case_insensitive(self):
        """Test that column comparison is case-insensitive."""
        col1 = ColumnMetadata('name', 'TEXT', nullable=True)
        col2 = ColumnMetadata('NAME', 'text', nullable=True)
        self.assertEqual(col1, col2)

    def test_column_inequality_different_nullable(self):
        """Test that columns are not equal if nullable differs."""
        col1 = ColumnMetadata('name', 'TEXT', nullable=True)
        col2 = ColumnMetadata('name', 'TEXT', nullable=False)
        self.assertNotEqual(col1, col2)


class TestForeignKeyMetadata(unittest.TestCase):
    """Test ForeignKeyMetadata equality."""

    def test_fk_equality(self):
        """Test that foreign keys are equal when all fields match."""
        fk1 = ForeignKeyMetadata('col', 'table', 'id')
        fk2 = ForeignKeyMetadata('col', 'table', 'id')
        self.assertEqual(fk1, fk2)

    def test_fk_equality_case_insensitive(self):
        """Test that foreign key comparison is case-insensitive."""
        fk1 = ForeignKeyMetadata('col', 'table', 'id')
        fk2 = ForeignKeyMetadata('COL', 'TABLE', 'ID')
        self.assertEqual(fk1, fk2)


if __name__ == '__main__':
    unittest.main()
