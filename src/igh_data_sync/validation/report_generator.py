"""Generate validation reports in JSON and Markdown formats."""

import json
import pathlib
from datetime import datetime, timezone

from ..type_mapping import SchemaDifference, TableSchema

# Maximum number of errors to display in summary
MAX_ERRORS_DISPLAYED = 10


class ReportGenerator:
    """Generates schema validation reports."""

    @staticmethod
    def generate_json_report(
        differences: list[SchemaDifference],
        dataverse_schemas: dict[str, TableSchema],
        database_schemas: dict[str, TableSchema],
        output_path: str = "schema_validation_report.json",
    ) -> None:
        """
        Generate JSON report of schema validation results.

        Args:
            differences: List of detected differences
            dataverse_schemas: Schemas from Dataverse
            database_schemas: Schemas from database
            output_path: Path to write JSON report
        """
        report = {
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "summary": {
                "total_entities_checked": len(dataverse_schemas),
                "total_differences": len(differences),
                "errors": sum(1 for d in differences if d.severity == "error"),
                "warnings": sum(1 for d in differences if d.severity == "warning"),
                "info": sum(1 for d in differences if d.severity == "info"),
            },
            "differences": [
                {
                    "entity": d.entity,
                    "issue_type": d.issue_type,
                    "severity": d.severity,
                    "description": d.description,
                    "details": d.details,
                }
                for d in differences
            ],
            "statistics": {
                "entities_in_dataverse": len(dataverse_schemas),
                "entities_in_database": len(database_schemas),
                "entities_matched": len(
                    set(dataverse_schemas.keys()) & set(database_schemas.keys()),
                ),
                "entities_missing_in_db": len(
                    set(dataverse_schemas.keys()) - set(database_schemas.keys()),
                ),
                "entities_extra_in_db": len(
                    set(database_schemas.keys()) - set(dataverse_schemas.keys()),
                ),
            },
        }

        with pathlib.Path(output_path).open("w", encoding="utf-8") as f:
            json.dump(report, f, indent=2)

        print(f"JSON report saved to: {output_path}")

    @staticmethod
    def _build_report_header() -> list[str]:
        """Build report header section."""
        return [
            "# Schema Validation Report",
            "",
            f"**Generated:** {datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M:%S UTC')}",
            "",
        ]

    @staticmethod
    def _build_summary_section(
        differences: list[SchemaDifference],
        dataverse_schemas: dict,
        errors,
        warnings,
        info,
    ) -> list[str]:
        """Build summary section of report."""
        return [
            "## Summary",
            "",
            f"- **Total Entities Checked:** {len(dataverse_schemas)}",
            f"- **Total Issues Found:** {len(differences)}",
            f"  - Errors: {len(errors)}",
            f"  - Warnings: {len(warnings)}",
            f"  - Info: {len(info)}",
            "",
        ]

    @staticmethod
    def _build_statistics_section(
        dataverse_schemas: dict,
        database_schemas: dict,
    ) -> list[str]:
        """Build statistics section of report."""
        dv_keys = set(dataverse_schemas.keys())
        db_keys = set(database_schemas.keys())

        return [
            "## Statistics",
            "",
            f"- **Entities in Dataverse:** {len(dataverse_schemas)}",
            f"- **Entities in Database:** {len(database_schemas)}",
            f"- **Entities Matched:** {len(dv_keys & db_keys)}",
            f"- **Entities Missing in DB:** {len(dv_keys - db_keys)}",
            f"- **Entities Extra in DB:** {len(db_keys - dv_keys)}",
            "",
        ]

    @staticmethod
    def _build_validation_result(errors) -> list[str]:
        """Build validation result section."""
        lines = ["## Validation Result", ""]
        if len(errors) == 0:
            lines.append("✅ **PASSED** - No critical errors found")
        else:
            lines.append(f"❌ **FAILED** - {len(errors)} critical error(s) found")
        lines.append("")
        return lines

    @staticmethod
    def _format_diff_group(diffs, severity_emoji: str) -> list[str]:
        """Format a group of diffs with given severity emoji."""
        lines = []
        for diff in diffs:
            lines.append(f"- {severity_emoji} **{diff.issue_type}**: {diff.description}")
            if diff.details:
                for key, value in diff.details.items():
                    lines.append(f"  - {key}: `{value}`")
        lines.append("")
        return lines

    @staticmethod
    def _build_detailed_issues(
        differences: list[SchemaDifference],
        by_entity: dict,
    ) -> list[str]:
        """Build detailed issues section of report."""
        if not differences:
            return ["## No Issues Found", "", "All schemas match perfectly!", ""]

        lines = ["## Detailed Issues", ""]

        # Sort entities by name
        for entity in sorted(by_entity.keys()):
            entity_diffs = by_entity[entity]
            lines.append(f"### {entity}")
            lines.append("")

            # Group by severity
            entity_errors = [d for d in entity_diffs if d.severity == "error"]
            entity_warnings = [d for d in entity_diffs if d.severity == "warning"]
            entity_info = [d for d in entity_diffs if d.severity == "info"]

            if entity_errors:
                lines.append("**Errors:**")
                lines.append("")
                lines.extend(ReportGenerator._format_diff_group(entity_errors, "❌"))

            if entity_warnings:
                lines.append("**Warnings:**")
                lines.append("")
                lines.extend(ReportGenerator._format_diff_group(entity_warnings, "⚠️"))

            if entity_info:
                lines.append("**Info:**")
                lines.append("")
                lines.extend(ReportGenerator._format_diff_group(entity_info, "ℹ️"))  # noqa: RUF001 - info emoji for user-facing output

        return lines

    @staticmethod
    def generate_markdown_report(
        differences: list[SchemaDifference],
        dataverse_schemas: dict[str, TableSchema],
        database_schemas: dict[str, TableSchema],
        output_path: str = "schema_validation_report.md",
    ) -> None:
        """
        Generate human-readable Markdown report.

        Args:
            differences: List of detected differences
            dataverse_schemas: Schemas from Dataverse
            database_schemas: Schemas from database
            output_path: Path to write Markdown report
        """
        # Calculate statistics
        errors = [d for d in differences if d.severity == "error"]
        warnings = [d for d in differences if d.severity == "warning"]
        info = [d for d in differences if d.severity == "info"]

        # Group differences by entity
        by_entity = {}
        for diff in differences:
            if diff.entity not in by_entity:
                by_entity[diff.entity] = []
            by_entity[diff.entity].append(diff)

        # Build report sections
        lines = []
        lines.extend(ReportGenerator._build_report_header())
        lines.extend(
            ReportGenerator._build_summary_section(differences, dataverse_schemas, errors, warnings, info),
        )
        lines.extend(ReportGenerator._build_statistics_section(dataverse_schemas, database_schemas))
        lines.extend(ReportGenerator._build_validation_result(errors))
        lines.extend(ReportGenerator._build_detailed_issues(differences, by_entity))

        # Write report
        with pathlib.Path(output_path).open("w", encoding="utf-8") as f:
            f.write("\n".join(lines))

        print(f"Markdown report saved to: {output_path}")

    @staticmethod
    def print_summary(
        differences: list[SchemaDifference],
        dataverse_schemas: dict[str, TableSchema],
        _database_schemas: dict[str, TableSchema],
    ) -> bool:
        """
        Print summary to console and return pass/fail status.

        Args:
            differences: List of detected differences
            dataverse_schemas: Schemas from Dataverse
            _database_schemas: Schemas from database (unused, kept for API compatibility)

        Returns:
            True if validation passed (no errors), False otherwise
        """
        errors = [d for d in differences if d.severity == "error"]
        warnings = [d for d in differences if d.severity == "warning"]
        info = [d for d in differences if d.severity == "info"]

        print("\n" + "=" * 60)
        print("SCHEMA VALIDATION SUMMARY")
        print("=" * 60)
        print(f"Entities checked: {len(dataverse_schemas)}")
        print(f"Total issues: {len(differences)}")
        print(f"  - Errors:   {len(errors)}")
        print(f"  - Warnings: {len(warnings)}")
        print(f"  - Info:     {len(info)}")
        print("=" * 60)

        if len(errors) == 0:
            print("✅ VALIDATION PASSED - No critical errors")
            print("=" * 60)
            return True
        else:
            print(f"❌ VALIDATION FAILED - {len(errors)} critical error(s)")
            print("=" * 60)
            print("\nCritical Errors:")
            for i, error in enumerate(errors[:MAX_ERRORS_DISPLAYED], 1):
                print(f"{i}. [{error.entity}] {error.description}")
            if len(errors) > MAX_ERRORS_DISPLAYED:
                print(f"... and {len(errors) - MAX_ERRORS_DISPLAYED} more errors")
            print("=" * 60)
            return False
