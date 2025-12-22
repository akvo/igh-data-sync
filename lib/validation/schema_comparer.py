"""Compare Dataverse and database schemas to detect differences."""

from ..type_mapping import (
    SchemaDifference,
    TableSchema,
    normalize_db_type,
)


class SchemaComparer:
    """Compares Dataverse and database schemas to detect differences."""

    def __init__(self, target_db: str = "sqlite"):
        """
        Initialize schema comparer.

        Args:
            target_db: Target database type for type normalization
        """
        self.target_db = target_db

    def compare_all(
        self,
        dataverse_schemas: dict[str, TableSchema],
        database_schemas: dict[str, TableSchema],
    ) -> list[SchemaDifference]:
        """
        Compare all schemas and detect differences.

        Args:
            dataverse_schemas: Schemas from Dataverse $metadata
            database_schemas: Schemas from actual database

        Returns:
            List of SchemaDifference objects
        """
        differences = []

        # Check for missing tables
        differences.extend(SchemaComparer._check_missing_tables(dataverse_schemas, database_schemas))

        # Check for extra tables (in database but not in Dataverse)
        differences.extend(SchemaComparer._check_extra_tables(dataverse_schemas, database_schemas))

        # Compare existing tables
        for entity_name, dv_schema in dataverse_schemas.items():
            if entity_name in database_schemas:
                # dv_schema already assigned from .items()
                db_schema = database_schemas[entity_name]

                # Compare columns
                differences.extend(self._compare_columns(entity_name, dv_schema, db_schema))

                # Compare primary keys
                differences.extend(SchemaComparer._compare_primary_keys(entity_name, dv_schema, db_schema))

                # Compare foreign keys
                differences.extend(SchemaComparer._compare_foreign_keys(entity_name, dv_schema, db_schema))

        return differences

    @staticmethod
    def _check_missing_tables(
        dataverse_schemas: dict[str, TableSchema],
        database_schemas: dict[str, TableSchema],
    ) -> list[SchemaDifference]:
        """Check for tables that exist in Dataverse but not in database."""

        differences = [
            SchemaDifference(
                entity=entity_name,
                issue_type="missing_table",
                severity="info",  # New entity - will be created
                description=(f"Table '{entity_name}' exists in Dataverse but not in database"),
                details={"entity_name": entity_name},
            )
            for entity_name in dataverse_schemas
            if entity_name not in database_schemas
        ]

        return differences

    @staticmethod
    def _check_extra_tables(
        dataverse_schemas: dict[str, TableSchema],
        database_schemas: dict[str, TableSchema],
    ) -> list[SchemaDifference]:
        """Check for tables that exist in database but not in Dataverse."""

        differences = [
            SchemaDifference(
                entity=entity_name,
                issue_type="extra_table",
                severity="warning",
                description=(f"Table '{entity_name}' exists in database but not in Dataverse schema"),
                details={"entity_name": entity_name},
            )
            for entity_name in database_schemas
            if entity_name not in dataverse_schemas
        ]

        return differences

    def _compare_columns(
        self,
        entity_name: str,
        dv_schema: TableSchema,
        db_schema: TableSchema,
    ) -> list[SchemaDifference]:
        """Compare columns between Dataverse and database schemas."""
        differences = []

        # Create column maps for easier lookup
        dv_columns = {col.name.lower(): col for col in dv_schema.columns}
        db_columns = {col.name.lower(): col for col in db_schema.columns}

        # Check for missing columns
        for col_name, dv_col in dv_columns.items():
            if col_name not in db_columns:
                differences.append(
                    SchemaDifference(
                        entity=entity_name,
                        issue_type="missing_column",
                        severity="info",  # New column in Dataverse - stored in json_response
                        description=f"Column '{dv_col.name}' missing in database",
                        details={
                            "column_name": dv_col.name,
                            "expected_type": dv_col.db_type,
                            "edm_type": dv_col.edm_type,
                        },
                    ),
                )

        # Check for extra columns
        for col_name, db_col in db_columns.items():
            if col_name not in dv_columns:
                differences.append(
                    SchemaDifference(
                        entity=entity_name,
                        issue_type="extra_column",
                        severity="warning",
                        description=(f"Column '{db_col.name}' exists in database but not in Dataverse"),
                        details={"column_name": db_col.name, "actual_type": db_col.db_type},
                    ),
                )

        # Check for type mismatches in existing columns
        for col_name, dv_col in dv_columns.items():
            if col_name in db_columns:
                # dv_col already assigned from .items()
                db_col = db_columns[col_name]

                # Normalize types for comparison
                dv_type_normalized = normalize_db_type(dv_col.db_type, self.target_db)
                db_type_normalized = normalize_db_type(db_col.db_type, self.target_db)

                if dv_type_normalized != db_type_normalized:
                    differences.append(
                        SchemaDifference(
                            entity=entity_name,
                            issue_type="type_mismatch",
                            severity="error",
                            description=f"Column '{dv_col.name}' type mismatch",
                            details={
                                "column_name": dv_col.name,
                                "expected_type": dv_col.db_type,
                                "actual_type": db_col.db_type,
                                "expected_normalized": dv_type_normalized,
                                "actual_normalized": db_type_normalized,
                                "edm_type": dv_col.edm_type,
                            },
                        ),
                    )

                # Check nullable mismatch (less severe)
                if dv_col.nullable != db_col.nullable:
                    differences.append(
                        SchemaDifference(
                            entity=entity_name,
                            issue_type="nullable_mismatch",
                            severity="warning",
                            description=f"Column '{dv_col.name}' nullable mismatch",
                            details={
                                "column_name": dv_col.name,
                                "expected_nullable": dv_col.nullable,
                                "actual_nullable": db_col.nullable,
                            },
                        ),
                    )

        return differences

    @staticmethod
    def _compare_primary_keys(
        entity_name: str,
        dv_schema: TableSchema,
        db_schema: TableSchema,
    ) -> list[SchemaDifference]:
        """Compare primary keys between Dataverse and database schemas."""
        differences = []

        # Normalize to lowercase for comparison
        dv_pk = dv_schema.primary_key.lower() if dv_schema.primary_key else None
        db_pk = db_schema.primary_key.lower() if db_schema.primary_key else None

        if dv_pk != db_pk:
            differences.append(
                SchemaDifference(
                    entity=entity_name,
                    issue_type="pk_mismatch",
                    severity="error",
                    description="Primary key mismatch",
                    details={
                        "expected_pk": dv_schema.primary_key,
                        "actual_pk": db_schema.primary_key,
                    },
                ),
            )

        return differences

    @staticmethod
    def _compare_foreign_keys(
        entity_name: str,
        dv_schema: TableSchema,
        db_schema: TableSchema,
    ) -> list[SchemaDifference]:
        """Compare foreign keys between Dataverse and database schemas."""
        differences = []

        # Create FK maps for easier lookup (keyed by column name)
        dv_fks = {fk.column.lower(): fk for fk in dv_schema.foreign_keys}
        db_fks = {fk.column.lower(): fk for fk in db_schema.foreign_keys}

        # Check for missing foreign keys
        for fk_col, dv_fk in dv_fks.items():
            if fk_col not in db_fks:
                differences.append(
                    SchemaDifference(
                        entity=entity_name,
                        issue_type="fk_missing",
                        severity="info",  # FK constraints not created - relationships queryable via JOIN
                        description=f"Column '{dv_fk.column}' has no FK constraint (use JOIN to query relationship)",
                        details={
                            "column": dv_fk.column,
                            "expected_references": (f"{dv_fk.referenced_table}.{dv_fk.referenced_column}"),
                        },
                    ),
                )
            else:
                # FK exists, check if it references the correct table/column
                db_fk = db_fks[fk_col]

                if (
                    dv_fk.referenced_table.lower() != db_fk.referenced_table.lower()
                    or dv_fk.referenced_column.lower() != db_fk.referenced_column.lower()
                ):
                    differences.append(
                        SchemaDifference(
                            entity=entity_name,
                            issue_type="fk_mismatch",
                            severity="warning",
                            description=(f"Foreign key on column '{dv_fk.column}' references wrong table/column"),
                            details={
                                "column": dv_fk.column,
                                "expected_references": (f"{dv_fk.referenced_table}.{dv_fk.referenced_column}"),
                                "actual_references": (f"{db_fk.referenced_table}.{db_fk.referenced_column}"),
                            },
                        ),
                    )

        # Check for extra foreign keys
        for fk_col, db_fk in db_fks.items():
            if fk_col not in dv_fks:
                differences.append(
                    SchemaDifference(
                        entity=entity_name,
                        issue_type="fk_extra",
                        severity="info",
                        description=f"Extra foreign key on column '{db_fk.column}'",
                        details={
                            "column": db_fk.column,
                            "actual_references": (f"{db_fk.referenced_table}.{db_fk.referenced_column}"),
                        },
                    ),
                )

        return differences
