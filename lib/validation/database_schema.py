"""Query database schemas from SQLite or PostgreSQL."""
import sqlite3
from typing import Dict, List
from ..type_mapping import TableSchema, ColumnMetadata, ForeignKeyMetadata
from ..config import Config


class DatabaseSchemaQuery:
    """Queries database schemas from SQLite or PostgreSQL."""

    def __init__(self, config: Config, db_type: str = None):
        """
        Initialize database schema query.

        Args:
            config: Configuration with database connection info
            db_type: Override database type ('sqlite' or 'postgresql')
        """
        self.config = config
        self.db_type = db_type or config.get_db_type()

    def query_all_schemas(self, entity_names: List[str]) -> Dict[str, TableSchema]:
        """
        Query schemas for all specified entities.

        Args:
            entity_names: List of table/entity names to query

        Returns:
            Dict mapping entity name to TableSchema

        Raises:
            RuntimeError: If database query fails
        """
        if self.db_type == 'sqlite':
            return self._query_sqlite_schemas(entity_names)
        elif self.db_type in ('postgresql', 'postgres'):
            return self._query_postgresql_schemas(entity_names)
        else:
            raise ValueError(f"Unsupported database type: {self.db_type}")

    def _query_sqlite_schemas(self, entity_names: List[str]) -> Dict[str, TableSchema]:
        """
        Query schemas from SQLite database using PRAGMA commands.

        Args:
            entity_names: List of table names

        Returns:
            Dict mapping table name to TableSchema
        """
        if not self.config.sqlite_db_path:
            raise RuntimeError("No SQLite database path configured")

        try:
            conn = sqlite3.connect(self.config.sqlite_db_path)
            cursor = conn.cursor()

            schemas = {}

            for entity_name in entity_names:
                # Check if table exists
                cursor.execute(
                    "SELECT name FROM sqlite_master WHERE type='table' AND name=?",
                    (entity_name,)
                )
                if not cursor.fetchone():
                    # Table doesn't exist
                    continue

                # Get column information
                columns = []
                cursor.execute(f"PRAGMA table_info('{entity_name}')")
                for row in cursor.fetchall():
                    # row format: (cid, name, type, notnull, dflt_value, pk)
                    col_name = row[1]
                    col_type = row[2]
                    not_null = row[3] == 1
                    is_pk = row[5] == 1

                    column = ColumnMetadata(
                        name=col_name,
                        db_type=col_type,
                        nullable=not not_null
                    )
                    columns.append(column)

                # Get primary key (from pk column in table_info)
                primary_key = None
                cursor.execute(f"PRAGMA table_info('{entity_name}')")
                for row in cursor.fetchall():
                    if row[5] == 1:  # pk column
                        primary_key = row[1]
                        break

                # Get foreign keys
                foreign_keys = []
                cursor.execute(f"PRAGMA foreign_key_list('{entity_name}')")
                for row in cursor.fetchall():
                    # row format: (id, seq, table, from, to, on_update, on_delete, match)
                    fk = ForeignKeyMetadata(
                        column=row[3],
                        referenced_table=row[2],
                        referenced_column=row[4]
                    )
                    foreign_keys.append(fk)

                schema = TableSchema(
                    entity_name=entity_name,
                    columns=columns,
                    primary_key=primary_key,
                    foreign_keys=foreign_keys
                )

                schemas[entity_name] = schema

            conn.close()
            return schemas

        except sqlite3.Error as e:
            raise RuntimeError(f"SQLite query failed: {e}")

    def _query_postgresql_schemas(self, entity_names: List[str]) -> Dict[str, TableSchema]:
        """
        Query schemas from PostgreSQL database using information_schema.

        Args:
            entity_names: List of table names

        Returns:
            Dict mapping table name to TableSchema
        """
        try:
            import psycopg2
        except ImportError:
            raise RuntimeError(
                "psycopg2 not installed. Install with: pip install psycopg2-binary"
            )

        if not self.config.postgres_connection_string:
            raise RuntimeError("No PostgreSQL connection string configured")

        try:
            conn = psycopg2.connect(self.config.postgres_connection_string)
            cursor = conn.cursor()

            schemas = {}

            for entity_name in entity_names:
                # Check if table exists
                cursor.execute(
                    """
                    SELECT table_name
                    FROM information_schema.tables
                    WHERE table_name = %s AND table_schema = 'public'
                    """,
                    (entity_name,)
                )
                if not cursor.fetchone():
                    # Table doesn't exist
                    continue

                # Get column information
                columns = []
                cursor.execute(
                    """
                    SELECT column_name, data_type, is_nullable, character_maximum_length
                    FROM information_schema.columns
                    WHERE table_name = %s AND table_schema = 'public'
                    ORDER BY ordinal_position
                    """,
                    (entity_name,)
                )

                for row in cursor.fetchall():
                    col_name = row[0]
                    col_type = row[1]
                    nullable = row[2] == 'YES'
                    max_length = row[3]

                    column = ColumnMetadata(
                        name=col_name,
                        db_type=col_type,
                        nullable=nullable,
                        max_length=max_length
                    )
                    columns.append(column)

                # Get primary key
                primary_key = None
                cursor.execute(
                    """
                    SELECT kcu.column_name
                    FROM information_schema.table_constraints tc
                    JOIN information_schema.key_column_usage kcu
                        ON tc.constraint_name = kcu.constraint_name
                    WHERE tc.table_name = %s
                        AND tc.constraint_type = 'PRIMARY KEY'
                        AND tc.table_schema = 'public'
                    """,
                    (entity_name,)
                )
                pk_row = cursor.fetchone()
                if pk_row:
                    primary_key = pk_row[0]

                # Get foreign keys
                foreign_keys = []
                cursor.execute(
                    """
                    SELECT
                        kcu.column_name,
                        ccu.table_name AS foreign_table_name,
                        ccu.column_name AS foreign_column_name
                    FROM information_schema.table_constraints AS tc
                    JOIN information_schema.key_column_usage AS kcu
                        ON tc.constraint_name = kcu.constraint_name
                        AND tc.table_schema = kcu.table_schema
                    JOIN information_schema.constraint_column_usage AS ccu
                        ON ccu.constraint_name = tc.constraint_name
                        AND ccu.table_schema = tc.table_schema
                    WHERE tc.constraint_type = 'FOREIGN KEY'
                        AND tc.table_name = %s
                        AND tc.table_schema = 'public'
                    """,
                    (entity_name,)
                )

                for row in cursor.fetchall():
                    fk = ForeignKeyMetadata(
                        column=row[0],
                        referenced_table=row[1],
                        referenced_column=row[2]
                    )
                    foreign_keys.append(fk)

                schema = TableSchema(
                    entity_name=entity_name,
                    columns=columns,
                    primary_key=primary_key,
                    foreign_keys=foreign_keys
                )

                schemas[entity_name] = schema

            conn.close()
            return schemas

        except Exception as e:
            raise RuntimeError(f"PostgreSQL query failed: {e}")
