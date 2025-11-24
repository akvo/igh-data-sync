"""Type mapping and data structures for Dataverse schema validation."""
from dataclasses import dataclass, field
from typing import List, Dict, Optional, Any


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
            self.name.lower() == other.name.lower() and
            self.db_type.upper() == other.db_type.upper() and
            self.nullable == other.nullable and
            self.max_length == other.max_length
        )


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
            self.column.lower() == other.column.lower() and
            self.referenced_table.lower() == other.referenced_table.lower() and
            self.referenced_column.lower() == other.referenced_column.lower()
        )


@dataclass
class IndexMetadata:
    """Metadata for a database index."""
    name: str
    columns: List[str]
    is_unique: bool = False


@dataclass
class TableSchema:
    """Complete schema for a table/entity."""
    entity_name: str
    columns: List[ColumnMetadata] = field(default_factory=list)
    primary_key: Optional[str] = None
    foreign_keys: List[ForeignKeyMetadata] = field(default_factory=list)
    indexes: List[IndexMetadata] = field(default_factory=list)


@dataclass
class SchemaDifference:
    """Represents a difference between Dataverse and database schemas."""
    entity: str
    issue_type: str  # 'missing_table', 'missing_column', 'extra_column', 'type_mismatch', 'pk_mismatch', 'fk_missing', etc.
    severity: str  # 'error', 'warning', 'info'
    description: str
    details: Dict[str, Any] = field(default_factory=dict)


# Edm type to SQLite type mapping
EDM_TYPE_MAP_SQLITE = {
    'Edm.String': 'TEXT',
    'Edm.Int16': 'INTEGER',
    'Edm.Int32': 'INTEGER',
    'Edm.Int64': 'INTEGER',
    'Edm.Decimal': 'REAL',
    'Edm.Double': 'REAL',
    'Edm.Boolean': 'INTEGER',
    'Edm.DateTimeOffset': 'TEXT',
    'Edm.Date': 'TEXT',
    'Edm.TimeOfDay': 'TEXT',
    'Edm.Guid': 'TEXT',
    'Edm.Binary': 'BLOB',
}

# Edm type to PostgreSQL type mapping
EDM_TYPE_MAP_POSTGRESQL = {
    'Edm.String': 'VARCHAR',
    'Edm.Int16': 'SMALLINT',
    'Edm.Int32': 'INTEGER',
    'Edm.Int64': 'BIGINT',
    'Edm.Decimal': 'NUMERIC',
    'Edm.Double': 'DOUBLE PRECISION',
    'Edm.Boolean': 'BOOLEAN',
    'Edm.DateTimeOffset': 'TIMESTAMP WITH TIME ZONE',
    'Edm.Date': 'DATE',
    'Edm.TimeOfDay': 'TIME',
    'Edm.Guid': 'UUID',
    'Edm.Binary': 'BYTEA',
}


def map_edm_to_db_type(edm_type: str, target_db: str, max_length: Optional[int] = None) -> str:
    """
    Map an Edm type to a database type.

    Args:
        edm_type: The OData Edm type (e.g., 'Edm.String')
        target_db: Target database type ('sqlite' or 'postgresql')
        max_length: Maximum length for string types

    Returns:
        The corresponding database type
    """
    if target_db.lower() == 'sqlite':
        type_map = EDM_TYPE_MAP_SQLITE
    elif target_db.lower() in ('postgresql', 'postgres'):
        type_map = EDM_TYPE_MAP_POSTGRESQL
    else:
        raise ValueError(f"Unsupported database type: {target_db}")

    base_type = type_map.get(edm_type, 'TEXT')

    # For PostgreSQL VARCHAR, add length if specified
    if target_db.lower() in ('postgresql', 'postgres'):
        if base_type == 'VARCHAR':
            if max_length:
                return f'VARCHAR({max_length})'
            else:
                # If no max_length specified, use TEXT for unlimited
                return 'TEXT'

    return base_type


def normalize_db_type(db_type: str, target_db: str) -> str:
    """
    Normalize database type for comparison.

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
    db_type = db_type.upper().strip()

    # Remove length specifications for comparison
    if '(' in db_type:
        db_type = db_type.split('(')[0].strip()

    if target_db.lower() == 'sqlite':
        # SQLite type affinity normalization
        if db_type in ('VARCHAR', 'CHAR', 'NVARCHAR', 'NCHAR', 'CLOB'):
            return 'TEXT'
        elif db_type in ('INT', 'TINYINT', 'SMALLINT', 'MEDIUMINT', 'BIGINT'):
            return 'INTEGER'
        elif db_type in ('DOUBLE', 'FLOAT', 'NUMERIC', 'DECIMAL'):
            return 'REAL'
        elif db_type in ('BLOB', 'BINARY', 'VARBINARY'):
            return 'BLOB'

    elif target_db.lower() in ('postgresql', 'postgres'):
        # PostgreSQL type normalization
        if db_type in ('CHARACTER VARYING', 'CHAR', 'CHARACTER', 'VARCHAR'):
            return 'TEXT'  # Normalize to TEXT for comparison
        elif db_type in ('INT', 'INT4'):
            return 'INTEGER'
        elif db_type in ('INT2',):
            return 'SMALLINT'
        elif db_type in ('INT8',):
            return 'BIGINT'
        elif db_type in ('FLOAT8', 'DOUBLE PRECISION'):
            return 'DOUBLE PRECISION'
        elif db_type in ('FLOAT4',):
            return 'REAL'
        elif db_type in ('BOOL',):
            return 'BOOLEAN'
        elif db_type in ('TIMESTAMPTZ',):
            return 'TIMESTAMP WITH TIME ZONE'

    return db_type
