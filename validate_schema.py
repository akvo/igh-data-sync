#!/usr/bin/env python3
"""
Schema Validator for Dataverse

Validates that the local database schema matches the Dataverse entity schemas
by comparing against the authoritative $metadata XML.

Usage:
    python validate_schema.py [--db-type sqlite|postgresql]

Exit codes:
    0 - Validation passed (no errors)
    1 - Validation failed (errors detected)
"""

import argparse
import asyncio
import sys
import traceback

from lib.auth import DataverseAuth
from lib.config import load_config, load_entities
from lib.dataverse_client import DataverseClient
from lib.validation.database_schema import DatabaseSchemaQuery
from lib.validation.dataverse_schema import DataverseSchemaFetcher
from lib.validation.report_generator import ReportGenerator
from lib.validation.schema_comparer import SchemaComparer


async def main():
    """Main validation workflow."""
    # Parse command-line arguments
    parser = argparse.ArgumentParser(description="Validate Dataverse schema against database")
    parser.add_argument(
        "--db-type",
        choices=["sqlite", "postgresql"],
        help="Database type (default: auto-detect from config)",
    )
    parser.add_argument(
        "--json-report",
        default="schema_validation_report.json",
        help="Path for JSON report (default: schema_validation_report.json)",
    )
    parser.add_argument(
        "--md-report",
        default="schema_validation_report.md",
        help="Path for Markdown report (default: schema_validation_report.md)",
    )
    args = parser.parse_args()

    print("=" * 60)
    print("DATAVERSE SCHEMA VALIDATOR")
    print("=" * 60)

    try:
        # [1/6] Load Configuration
        print("\n[1/6] Loading configuration...")
        config = load_config()
        entities = load_entities("entities_config.json")
        print("✓ Loaded configuration")
        print(f"  - API URL: {config.api_url}")
        print(f"  - Entities to check: {len(entities)}")

        # Determine database type
        db_type = args.db_type or config.get_db_type()
        print(f"  - Database type: {db_type}")

        # [2/6] Authenticate with Dataverse
        print("\n[2/6] Authenticating with Dataverse...")
        auth = DataverseAuth(config)
        token = auth.authenticate()
        print("✓ Successfully authenticated")
        print(f"  - Tenant ID: {auth.tenant_id}")

        # [3/6] Fetch Dataverse Schemas
        print("\n[3/6] Fetching Dataverse schemas from $metadata...")
        async with DataverseClient(config, token) as client:
            fetcher = DataverseSchemaFetcher(client, target_db=db_type)
            dataverse_schemas = await fetcher.fetch_schemas_from_metadata(entities)
        print(f"✓ Fetched {len(dataverse_schemas)} entity schemas from Dataverse")

        # [4/6] Query Database Schemas
        print("\n[4/6] Querying database schemas...")
        db_query = DatabaseSchemaQuery(config, db_type=db_type)
        database_schemas = db_query.query_all_schemas(entities)
        print(f"✓ Queried {len(database_schemas)} entity schemas from database")

        # [5/6] Compare Schemas
        print("\n[5/6] Comparing schemas...")
        comparer = SchemaComparer(target_db=db_type)
        differences = comparer.compare_all(dataverse_schemas, database_schemas)
        print(f"✓ Comparison complete - found {len(differences)} difference(s)")

        # [6/6] Generate Reports
        print("\n[6/6] Generating reports...")
        reporter = ReportGenerator()

        # Generate JSON report
        reporter.generate_json_report(
            differences,
            dataverse_schemas,
            database_schemas,
            output_path=args.json_report,
        )

        # Generate Markdown report
        reporter.generate_markdown_report(
            differences,
            dataverse_schemas,
            database_schemas,
            output_path=args.md_report,
        )

        # Print summary and get pass/fail status
        passed = reporter.print_summary(differences, dataverse_schemas, database_schemas)

        # Exit with appropriate code
        sys.exit(0 if passed else 1)

    except KeyboardInterrupt:
        print("\n\nInterrupted by user")
        sys.exit(1)
    except Exception as e:
        print(f"\n❌ ERROR: {e}", file=sys.stderr)
        traceback.print_exc()
        sys.exit(1)


if __name__ == "__main__":
    asyncio.run(main())
