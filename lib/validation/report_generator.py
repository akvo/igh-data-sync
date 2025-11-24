"""Generate validation reports in JSON and Markdown formats."""
import json
from datetime import datetime
from typing import Dict, List
from ..type_mapping import TableSchema, SchemaDifference


class ReportGenerator:
    """Generates schema validation reports."""

    def generate_json_report(
        self,
        differences: List[SchemaDifference],
        dataverse_schemas: Dict[str, TableSchema],
        database_schemas: Dict[str, TableSchema],
        output_path: str = 'schema_validation_report.json'
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
            'timestamp': datetime.utcnow().isoformat(),
            'summary': {
                'total_entities_checked': len(dataverse_schemas),
                'total_differences': len(differences),
                'errors': sum(1 for d in differences if d.severity == 'error'),
                'warnings': sum(1 for d in differences if d.severity == 'warning'),
                'info': sum(1 for d in differences if d.severity == 'info')
            },
            'differences': [
                {
                    'entity': d.entity,
                    'issue_type': d.issue_type,
                    'severity': d.severity,
                    'description': d.description,
                    'details': d.details
                }
                for d in differences
            ],
            'statistics': {
                'entities_in_dataverse': len(dataverse_schemas),
                'entities_in_database': len(database_schemas),
                'entities_matched': len(set(dataverse_schemas.keys()) & set(database_schemas.keys())),
                'entities_missing_in_db': len(set(dataverse_schemas.keys()) - set(database_schemas.keys())),
                'entities_extra_in_db': len(set(database_schemas.keys()) - set(dataverse_schemas.keys()))
            }
        }

        with open(output_path, 'w') as f:
            json.dump(report, f, indent=2)

        print(f"JSON report saved to: {output_path}")

    def generate_markdown_report(
        self,
        differences: List[SchemaDifference],
        dataverse_schemas: Dict[str, TableSchema],
        database_schemas: Dict[str, TableSchema],
        output_path: str = 'schema_validation_report.md'
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
        errors = [d for d in differences if d.severity == 'error']
        warnings = [d for d in differences if d.severity == 'warning']
        info = [d for d in differences if d.severity == 'info']

        # Group differences by entity
        by_entity = {}
        for diff in differences:
            if diff.entity not in by_entity:
                by_entity[diff.entity] = []
            by_entity[diff.entity].append(diff)

        # Build report
        lines = []
        lines.append("# Schema Validation Report")
        lines.append("")
        lines.append(f"**Generated:** {datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S UTC')}")
        lines.append("")

        # Summary
        lines.append("## Summary")
        lines.append("")
        lines.append(f"- **Total Entities Checked:** {len(dataverse_schemas)}")
        lines.append(f"- **Total Issues Found:** {len(differences)}")
        lines.append(f"  - Errors: {len(errors)}")
        lines.append(f"  - Warnings: {len(warnings)}")
        lines.append(f"  - Info: {len(info)}")
        lines.append("")

        # Statistics
        lines.append("## Statistics")
        lines.append("")
        lines.append(f"- **Entities in Dataverse:** {len(dataverse_schemas)}")
        lines.append(f"- **Entities in Database:** {len(database_schemas)}")
        lines.append(f"- **Entities Matched:** {len(set(dataverse_schemas.keys()) & set(database_schemas.keys()))}")
        lines.append(f"- **Entities Missing in DB:** {len(set(dataverse_schemas.keys()) - set(database_schemas.keys()))}")
        lines.append(f"- **Entities Extra in DB:** {len(set(database_schemas.keys()) - set(dataverse_schemas.keys()))}")
        lines.append("")

        # Validation Result
        lines.append("## Validation Result")
        lines.append("")
        if len(errors) == 0:
            lines.append("✅ **PASSED** - No critical errors found")
        else:
            lines.append(f"❌ **FAILED** - {len(errors)} critical error(s) found")
        lines.append("")

        # Detailed Issues
        if differences:
            lines.append("## Detailed Issues")
            lines.append("")

            # Sort entities by name
            for entity in sorted(by_entity.keys()):
                entity_diffs = by_entity[entity]

                lines.append(f"### {entity}")
                lines.append("")

                # Group by severity
                entity_errors = [d for d in entity_diffs if d.severity == 'error']
                entity_warnings = [d for d in entity_diffs if d.severity == 'warning']
                entity_info = [d for d in entity_diffs if d.severity == 'info']

                if entity_errors:
                    lines.append("**Errors:**")
                    lines.append("")
                    for diff in entity_errors:
                        lines.append(f"- ❌ **{diff.issue_type}**: {diff.description}")
                        if diff.details:
                            for key, value in diff.details.items():
                                lines.append(f"  - {key}: `{value}`")
                    lines.append("")

                if entity_warnings:
                    lines.append("**Warnings:**")
                    lines.append("")
                    for diff in entity_warnings:
                        lines.append(f"- ⚠️  **{diff.issue_type}**: {diff.description}")
                        if diff.details:
                            for key, value in diff.details.items():
                                lines.append(f"  - {key}: `{value}`")
                    lines.append("")

                if entity_info:
                    lines.append("**Info:**")
                    lines.append("")
                    for diff in entity_info:
                        lines.append(f"- ℹ️  **{diff.issue_type}**: {diff.description}")
                        if diff.details:
                            for key, value in diff.details.items():
                                lines.append(f"  - {key}: `{value}`")
                    lines.append("")

        else:
            lines.append("## No Issues Found")
            lines.append("")
            lines.append("All schemas match perfectly!")
            lines.append("")

        # Write report
        with open(output_path, 'w') as f:
            f.write('\n'.join(lines))

        print(f"Markdown report saved to: {output_path}")

    def print_summary(
        self,
        differences: List[SchemaDifference],
        dataverse_schemas: Dict[str, TableSchema],
        database_schemas: Dict[str, TableSchema]
    ) -> bool:
        """
        Print summary to console and return pass/fail status.

        Args:
            differences: List of detected differences
            dataverse_schemas: Schemas from Dataverse
            database_schemas: Schemas from database

        Returns:
            True if validation passed (no errors), False otherwise
        """
        errors = [d for d in differences if d.severity == 'error']
        warnings = [d for d in differences if d.severity == 'warning']
        info = [d for d in differences if d.severity == 'info']

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
            for i, error in enumerate(errors[:10], 1):  # Show first 10
                print(f"{i}. [{error.entity}] {error.description}")
            if len(errors) > 10:
                print(f"... and {len(errors) - 10} more errors")
            print("=" * 60)
            return False
