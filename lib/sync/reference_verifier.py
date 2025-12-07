"""Reference integrity verifier for synced data.

Checks for dangling foreign key references using LEFT JOIN queries.
"""

from dataclasses import dataclass, field

from .database import DatabaseManager
from .relationship_graph import RelationshipGraph

# Maximum number of sample IDs to display in verification report
MAX_SAMPLE_DISPLAY = 5


@dataclass
class VerificationIssue:
    """A single reference integrity issue."""

    table: str
    fk_column: str
    referenced_table: str
    dangling_count: int
    total_checked: int
    sample_ids: list[str] = field(default_factory=list)


@dataclass
class VerificationReport:
    """Report of reference integrity verification."""

    total_checks: int = 0
    total_issues: int = 0
    issues: list[VerificationIssue] = field(default_factory=list)

    def __str__(self) -> str:
        """Format report for display."""
        lines = ["", "=" * 60, "Reference Verification Report", "=" * 60, ""]

        if not self.issues:
            lines.append("✓ All references valid!")
            lines.append("")
            lines.append("Statistics:")
            lines.append(f"  Total references checked: {self.total_checks}")
            lines.append("  Dangling references: 0")
            lines.append("  Tables with issues: 0")
        else:
            lines.append(f"Found {self.total_issues} reference integrity issue(s):")
            lines.append("")

            for issue in self.issues:
                lines.append(
                    f"✗ {issue.table}.{issue.fk_column} → {issue.referenced_table}: "
                    f"{issue.dangling_count} dangling ({issue.total_checked} checked)",
                )
                if issue.sample_ids:
                    sample = ", ".join(
                        f"'{record_id}'" for record_id in issue.sample_ids[:MAX_SAMPLE_DISPLAY]
                    )
                    if len(issue.sample_ids) > MAX_SAMPLE_DISPLAY:
                        sample += f", ... ({len(issue.sample_ids) - MAX_SAMPLE_DISPLAY} more)"
                    lines.append(f"  Missing IDs: [{sample}]")

            lines.append("")
            lines.append(
                f"Summary: {len(self.issues)} table(s) with issues, "
                f"{self.total_issues} dangling references total",
            )

        lines.append("=" * 60)
        return "\n".join(lines)


class ReferenceVerifier:
    """
    Verifies reference integrity of synced data.

    Uses LEFT JOIN queries to detect dangling foreign key references.
    """

    def verify_references(
        self,
        db_manager: DatabaseManager,
        relationship_graph: RelationshipGraph,
    ) -> VerificationReport:
        """
        Check for dangling references using LEFT JOIN queries.

        Args:
            db_manager: Database manager
            relationship_graph: Relationship graph with FK information

        Returns:
            VerificationReport with any issues found

        Algorithm:
            1. For each entity in the relationship graph:
                a. Get all FK columns from 'references_to' relationships
                b. For each FK column:
                    - Build LEFT JOIN query to find dangling references
                    - Count non-null FKs where referenced record doesn't exist
                    - If count > 0: add to issues
            2. Return report with statistics
        """
        report = VerificationReport()

        if not db_manager.conn:
            db_manager.connect()

        # Check each entity's foreign keys
        for entity_api_name, relationships in relationship_graph.relationships.items():
            # Skip if table doesn't exist
            cursor = db_manager.conn.cursor()
            cursor.execute(
                "SELECT name FROM sqlite_master WHERE type='table' AND name=?",
                (entity_api_name,),
            )
            if not cursor.fetchone():
                continue

            # Check each foreign key
            for referenced_table, fk_column, referenced_column in relationships.references_to:
                report.total_checks += 1

                # Check if referenced table exists
                cursor.execute(
                    "SELECT name FROM sqlite_master WHERE type='table' AND name=?",
                    (referenced_table,),
                )
                if not cursor.fetchone():
                    # Referenced table doesn't exist - skip (might be intentional)
                    continue

                # Find dangling references using LEFT JOIN
                # Query: Find records where FK is not null but referenced record doesn't exist
                # S608: Table/column names are from EntityConfig and TableSchema, not user input
                # Use referenced_column from metadata (business key for SCD2, not surrogate key)
                query = f"""
                    SELECT
                        t.{fk_column},
                        COUNT(*) as ref_count
                    FROM {entity_api_name} t
                    LEFT JOIN {referenced_table} r
                        ON t.{fk_column} = r.{referenced_column}
                    WHERE t.{fk_column} IS NOT NULL
                        AND r.{referenced_column} IS NULL
                    GROUP BY t.{fk_column}
                """  # noqa: S608, SQL safe - table/column names from EntityConfig/TableSchema (not user input), values parameterized

                try:
                    cursor.execute(query)
                    dangling_refs = cursor.fetchall()

                    if dangling_refs:
                        # Count total dangling references
                        dangling_count = sum(row[1] for row in dangling_refs)
                        sample_ids = [row[0] for row in dangling_refs[:10]]

                        # Count total references checked
                        # S608: table/column names from EntityConfig/TableSchema, not user input
                        cursor.execute(
                            f"SELECT COUNT(*) FROM {entity_api_name} WHERE {fk_column} IS NOT NULL",  # noqa: S608
                        )
                        total_checked = cursor.fetchone()[0]

                        issue = VerificationIssue(
                            table=entity_api_name,
                            fk_column=fk_column,
                            referenced_table=referenced_table,
                            dangling_count=dangling_count,
                            total_checked=total_checked,
                            sample_ids=sample_ids,
                        )
                        report.issues.append(issue)
                        report.total_issues += dangling_count

                except Exception as e:
                    # Skip this FK if query fails (e.g., column doesn't exist)
                    print(f"  ⚠️  Warning: Could not verify {entity_api_name}.{fk_column}: {e}")
                    continue

        return report

    def _get_primary_key(self, db_manager: DatabaseManager, table_name: str) -> str:
        """
        Get primary key column name for a table.

        Args:
            db_manager: Database manager
            table_name: Table to get PK for

        Returns:
            Primary key column name (assumes first column if none found)
        """
        cursor = db_manager.conn.cursor()
        cursor.execute(f"PRAGMA table_info({table_name})")
        columns = cursor.fetchall()

        # Find column with pk=1
        for col in columns:
            if col[5] == 1:  # pk flag is at index 5
                return col[1]  # name is at index 1

        # Fallback: return first column
        if columns:
            return columns[0][1]

        msg = f"No columns found for table {table_name}"
        raise ValueError(msg)
