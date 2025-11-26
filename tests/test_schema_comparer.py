"""Tests for schema comparison logic."""
import unittest
from lib.type_mapping import (
    TableSchema, ColumnMetadata, ForeignKeyMetadata, SchemaDifference
)
from lib.validation.schema_comparer import SchemaComparer


class TestSchemaComparer(unittest.TestCase):
    """Test schema comparison logic."""

    def setUp(self):
        """Set up test fixtures."""
        self.comparer = SchemaComparer(target_db='sqlite')

    def test_no_differences_perfect_match(self):
        """Test that identical schemas produce no differences."""
        schema = TableSchema(
            entity_name='test_entity',
            columns=[
                ColumnMetadata('id', 'INTEGER', nullable=False),
                ColumnMetadata('name', 'TEXT', nullable=True)
            ],
            primary_key='id',
            foreign_keys=[]
        )

        dataverse_schemas = {'test_entity': schema}
        database_schemas = {'test_entity': schema}

        differences = self.comparer.compare_all(dataverse_schemas, database_schemas)
        self.assertEqual(len(differences), 0)

    def test_missing_table(self):
        """Test detection of missing table in database."""
        dv_schema = TableSchema(
            entity_name='missing_entity',
            columns=[ColumnMetadata('id', 'INTEGER')],
            primary_key='id'
        )

        dataverse_schemas = {'missing_entity': dv_schema}
        database_schemas = {}

        differences = self.comparer.compare_all(dataverse_schemas, database_schemas)

        self.assertEqual(len(differences), 1)
        self.assertEqual(differences[0].issue_type, 'missing_table')
        self.assertEqual(differences[0].severity, 'info')
        self.assertEqual(differences[0].entity, 'missing_entity')

    def test_extra_table(self):
        """Test detection of extra table in database."""
        db_schema = TableSchema(
            entity_name='extra_entity',
            columns=[ColumnMetadata('id', 'INTEGER')],
            primary_key='id'
        )

        dataverse_schemas = {}
        database_schemas = {'extra_entity': db_schema}

        differences = self.comparer.compare_all(dataverse_schemas, database_schemas)

        self.assertEqual(len(differences), 1)
        self.assertEqual(differences[0].issue_type, 'extra_table')
        self.assertEqual(differences[0].severity, 'warning')
        self.assertEqual(differences[0].entity, 'extra_entity')

    def test_missing_column(self):
        """Test detection of missing column in database."""
        dv_schema = TableSchema(
            entity_name='test_entity',
            columns=[
                ColumnMetadata('id', 'INTEGER'),
                ColumnMetadata('name', 'TEXT'),
                ColumnMetadata('missing_col', 'TEXT')
            ],
            primary_key='id'
        )

        db_schema = TableSchema(
            entity_name='test_entity',
            columns=[
                ColumnMetadata('id', 'INTEGER'),
                ColumnMetadata('name', 'TEXT')
            ],
            primary_key='id'
        )

        dataverse_schemas = {'test_entity': dv_schema}
        database_schemas = {'test_entity': db_schema}

        differences = self.comparer.compare_all(dataverse_schemas, database_schemas)

        missing_col_diffs = [d for d in differences if d.issue_type == 'missing_column']
        self.assertEqual(len(missing_col_diffs), 1)
        self.assertEqual(missing_col_diffs[0].severity, 'info')
        self.assertEqual(missing_col_diffs[0].details['column_name'], 'missing_col')

    def test_extra_column(self):
        """Test detection of extra column in database."""
        dv_schema = TableSchema(
            entity_name='test_entity',
            columns=[
                ColumnMetadata('id', 'INTEGER'),
                ColumnMetadata('name', 'TEXT')
            ],
            primary_key='id'
        )

        db_schema = TableSchema(
            entity_name='test_entity',
            columns=[
                ColumnMetadata('id', 'INTEGER'),
                ColumnMetadata('name', 'TEXT'),
                ColumnMetadata('extra_col', 'TEXT')
            ],
            primary_key='id'
        )

        dataverse_schemas = {'test_entity': dv_schema}
        database_schemas = {'test_entity': db_schema}

        differences = self.comparer.compare_all(dataverse_schemas, database_schemas)

        extra_col_diffs = [d for d in differences if d.issue_type == 'extra_column']
        self.assertEqual(len(extra_col_diffs), 1)
        self.assertEqual(extra_col_diffs[0].severity, 'warning')
        self.assertEqual(extra_col_diffs[0].details['column_name'], 'extra_col')

    def test_type_mismatch(self):
        """Test detection of column type mismatch."""
        dv_schema = TableSchema(
            entity_name='test_entity',
            columns=[
                ColumnMetadata('id', 'INTEGER'),
                ColumnMetadata('count', 'INTEGER')
            ],
            primary_key='id'
        )

        db_schema = TableSchema(
            entity_name='test_entity',
            columns=[
                ColumnMetadata('id', 'INTEGER'),
                ColumnMetadata('count', 'TEXT')  # Wrong type
            ],
            primary_key='id'
        )

        dataverse_schemas = {'test_entity': dv_schema}
        database_schemas = {'test_entity': db_schema}

        differences = self.comparer.compare_all(dataverse_schemas, database_schemas)

        type_mismatch_diffs = [d for d in differences if d.issue_type == 'type_mismatch']
        self.assertEqual(len(type_mismatch_diffs), 1)
        self.assertEqual(type_mismatch_diffs[0].severity, 'error')
        self.assertEqual(type_mismatch_diffs[0].details['column_name'], 'count')

    def test_primary_key_mismatch(self):
        """Test detection of primary key mismatch."""
        dv_schema = TableSchema(
            entity_name='test_entity',
            columns=[
                ColumnMetadata('id', 'INTEGER'),
                ColumnMetadata('name', 'TEXT')
            ],
            primary_key='id'
        )

        db_schema = TableSchema(
            entity_name='test_entity',
            columns=[
                ColumnMetadata('id', 'INTEGER'),
                ColumnMetadata('name', 'TEXT')
            ],
            primary_key='name'  # Wrong PK
        )

        dataverse_schemas = {'test_entity': dv_schema}
        database_schemas = {'test_entity': db_schema}

        differences = self.comparer.compare_all(dataverse_schemas, database_schemas)

        pk_diffs = [d for d in differences if d.issue_type == 'pk_mismatch']
        self.assertEqual(len(pk_diffs), 1)
        self.assertEqual(pk_diffs[0].severity, 'error')

    def test_missing_foreign_key(self):
        """Test detection of missing foreign key."""
        dv_schema = TableSchema(
            entity_name='test_entity',
            columns=[
                ColumnMetadata('id', 'INTEGER'),
                ColumnMetadata('parent_id', 'INTEGER')
            ],
            primary_key='id',
            foreign_keys=[
                ForeignKeyMetadata('parent_id', 'parent_table', 'id')
            ]
        )

        db_schema = TableSchema(
            entity_name='test_entity',
            columns=[
                ColumnMetadata('id', 'INTEGER'),
                ColumnMetadata('parent_id', 'INTEGER')
            ],
            primary_key='id',
            foreign_keys=[]  # Missing FK
        )

        dataverse_schemas = {'test_entity': dv_schema}
        database_schemas = {'test_entity': db_schema}

        differences = self.comparer.compare_all(dataverse_schemas, database_schemas)

        fk_diffs = [d for d in differences if d.issue_type == 'fk_missing']
        self.assertEqual(len(fk_diffs), 1)
        self.assertEqual(fk_diffs[0].severity, 'warning')


if __name__ == '__main__':
    unittest.main()
