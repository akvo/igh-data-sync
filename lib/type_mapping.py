"""Type mapping and data structures for Dataverse schema validation."""

from dataclasses import dataclass, field
from typing import Any, Optional


@dataclass
class ColumnMetadata:
    """Metadata for a single column."""

    name: str
    db_type: str
    edm_type: Optional[str] = None
    nullable: bool = True
    max_length: Optional[int] = None

    def __eq__(self, other):
        """Compare columns ignoring case differences in type names."""
        if not isinstance(other, ColumnMetadata):
            return False
        return (
            self.name.lower() == other.name.lower()
            and self.db_type.upper() == other.db_type.upper()
            and self.nullable == other.nullable
            and self.max_length == other.max_length
        )

    def __hash__(self):
        """Hash columns using case-normalized values to match __eq__."""
        return hash((self.name.lower(), self.db_type.upper(), self.nullable, self.max_length))


@dataclass
class ForeignKeyMetadata:
    """Metadata for a foreign key relationship."""

    column: str
    referenced_table: str
    referenced_column: str
    constraint_name: Optional[str] = None

    def __eq__(self, other):
        """Compare foreign keys ignoring case differences."""
        if not isinstance(other, ForeignKeyMetadata):
            return False
        return (
            self.column.lower() == other.column.lower()
            and self.referenced_table.lower() == other.referenced_table.lower()
            and self.referenced_column.lower() == other.referenced_column.lower()
        )

    def __hash__(self):
        """Hash foreign keys using case-normalized values to match __eq__."""
        return hash((self.column.lower(), self.referenced_table.lower(), self.referenced_column.lower()))


@dataclass
class IndexMetadata:
    """Metadata for a database index."""

    name: str
    columns: list[str]
    is_unique: bool = False


@dataclass
class TableSchema:
    """Complete schema for a table/entity."""

    entity_name: str
    columns: list[ColumnMetadata] = field(default_factory=list)
    primary_key: Optional[str] = None
    foreign_keys: list[ForeignKeyMetadata] = field(default_factory=list)
    indexes: list[IndexMetadata] = field(default_factory=list)


@dataclass
class SchemaDifference:
    """Represents a difference between Dataverse and database schemas."""

    entity: str
    # Issue types: 'missing_table', 'missing_column', 'extra_column', 'type_mismatch',
    # 'pk_mismatch', 'fk_missing'
    issue_type: str
    severity: str  # 'error', 'warning', 'info'
    description: str
    details: dict[str, Any] = field(default_factory=dict)


# Edm type to SQLite type mapping
EDM_TYPE_MAP_SQLITE = {
    "Edm.String": "TEXT",
    "Edm.Int16": "INTEGER",
    "Edm.Int32": "INTEGER",
    "Edm.Int64": "INTEGER",
    "Edm.Decimal": "REAL",
    "Edm.Double": "REAL",
    "Edm.Boolean": "INTEGER",
    "Edm.DateTimeOffset": "TEXT",
    "Edm.Date": "TEXT",
    "Edm.TimeOfDay": "TEXT",
    "Edm.Guid": "TEXT",
    "Edm.Binary": "BLOB",
}

# Edm type to PostgreSQL type mapping
EDM_TYPE_MAP_POSTGRESQL = {
    "Edm.String": "VARCHAR",
    "Edm.Int16": "SMALLINT",
    "Edm.Int32": "INTEGER",
    "Edm.Int64": "BIGINT",
    "Edm.Decimal": "NUMERIC",
    "Edm.Double": "DOUBLE PRECISION",
    "Edm.Boolean": "BOOLEAN",
    "Edm.DateTimeOffset": "TIMESTAMP WITH TIME ZONE",
    "Edm.Date": "DATE",
    "Edm.TimeOfDay": "TIME",
    "Edm.Guid": "UUID",
    "Edm.Binary": "BYTEA",
}


def map_edm_to_db_type(
    edm_type: str,
    target_db: str,
    max_length: Optional[int] = None,
    is_option_set: bool = False,
) -> str:
    """
    Map an Edm type to a database type.

    Args:
        edm_type: The OData Edm type (e.g., 'Edm.String')
        target_db: Target database type ('sqlite' or 'postgresql')
        max_length: Maximum length for string types
        is_option_set: If True and edm_type is Edm.String, return INTEGER
                       (option sets appear as Edm.String in metadata but store integer codes)

    Returns:
        The corresponding database type
    """
    # CRITICAL: Override for option sets
    # Option sets appear as Edm.String in metadata but store integer codes
    if is_option_set and edm_type == "Edm.String":
        return "INTEGER"

    if target_db.lower() == "sqlite":
        type_map = EDM_TYPE_MAP_SQLITE
    elif target_db.lower() in ("postgresql", "postgres"):
        type_map = EDM_TYPE_MAP_POSTGRESQL
    else:
        msg = f"Unsupported database type: {target_db}"
        raise ValueError(msg)

    base_type = type_map.get(edm_type, "TEXT")

    # For PostgreSQL VARCHAR, add length if specified
    if target_db.lower() in ("postgresql", "postgres") and base_type == "VARCHAR":
        if max_length:
            return f"VARCHAR({max_length})"
        else:
            # If no max_length specified, use TEXT for unlimited
            return "TEXT"

    return base_type


# Type alias mappings for database type normalization
SQLITE_TYPE_ALIASES = {
    "TEXT": {"VARCHAR", "CHAR", "NVARCHAR", "NCHAR", "CLOB"},
    "INTEGER": {"INT", "TINYINT", "SMALLINT", "MEDIUMINT", "BIGINT"},
    "REAL": {"DOUBLE", "FLOAT", "NUMERIC", "DECIMAL"},
    "BLOB": {"BLOB", "BINARY", "VARBINARY"},
}

POSTGRESQL_TYPE_ALIASES = {
    "TEXT": {"CHARACTER VARYING", "CHAR", "CHARACTER", "VARCHAR"},
    "INTEGER": {"INT", "INT4"},
    "SMALLINT": {"INT2"},
    "BIGINT": {"INT8"},
    "DOUBLE PRECISION": {"FLOAT8", "DOUBLE PRECISION"},
    "REAL": {"FLOAT4"},
    "BOOLEAN": {"BOOL"},
    "TIMESTAMP WITH TIME ZONE": {"TIMESTAMPTZ"},
}

TYPE_ALIASES = {
    "sqlite": SQLITE_TYPE_ALIASES,
    "postgresql": POSTGRESQL_TYPE_ALIASES,
    "postgres": POSTGRESQL_TYPE_ALIASES,
}


def normalize_db_type(db_type: str, target_db: str) -> str:
    """
    Normalize database type for comparison using dictionary-driven lookup.

    Handles variations like:
    - VARCHAR vs TEXT
    - INTEGER vs INT
    - REAL vs DOUBLE

    Args:
        db_type: The database type string
        target_db: Target database type ('sqlite' or 'postgresql')

    Returns:
        Normalized type string for comparison
    """
    db_type_clean = db_type.upper().strip()

    # Remove length specifications for comparison
    if "(" in db_type_clean:
        db_type_clean = db_type_clean.split("(")[0].strip()

    target_db_lower = target_db.lower()
    if target_db_lower not in TYPE_ALIASES:
        # Unknown database type - return as-is
        return db_type_clean

    # Look up type in aliases dictionary
    aliases = TYPE_ALIASES[target_db_lower]

    for canonical_type, variants in aliases.items():
        if db_type_clean in variants:
            return canonical_type

    # No alias found - return cleaned type as-is
    return db_type_clean
