#!/usr/bin/env python3
"""
Schema Validator for Dataverse

Validates that the local database schema matches the Dataverse entity schemas
by comparing against the authoritative $metadata XML.

Usage:
    python -m igh_data_sync.scripts.validate [--db-type sqlite|postgresql]

Exit codes:
    0 - Validation passed (no errors)
    1 - Validation failed (errors detected)
"""

import argparse
import asyncio
import sys
import traceback

from igh_data_sync.auth import DataverseAuth
from igh_data_sync.config import load_config, load_entities
from igh_data_sync.dataverse_client import DataverseClient
from igh_data_sync.validation.database_schema import DatabaseSchemaQuery
from igh_data_sync.validation.dataverse_schema import DataverseSchemaFetcher
from igh_data_sync.validation.report_generator import ReportGenerator
from igh_data_sync.validation.schema_comparer import SchemaComparer


async def async_main(db_type, json_report, md_report, entities_config, env_file):
    """Async validation workflow."""
    print("=" * 60)
    print("DATAVERSE SCHEMA VALIDATOR")
    print("=" * 60)

    try:
        # [1/6] Load Configuration
        print("\n[1/6] Loading configuration...")
        config = load_config(env_file=env_file)
        entities = load_entities(path=entities_config)
        print("✓ Loaded configuration")
        print(f"  - API URL: {config.api_url}")
        print(f"  - Entities to check: {len(entities)}")

        # Determine database type
        db_type = db_type or config.get_db_type()
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
            output_path=json_report,
        )

        # Generate Markdown report
        reporter.generate_markdown_report(
            differences,
            dataverse_schemas,
            database_schemas,
            output_path=md_report,
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


def main():
    """CLI entry point for validate-schema command."""
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
    parser.add_argument(
        "--entities-config",
        default=None,
        help="Path to entities config file (default: package data)",
    )
    parser.add_argument(
        "--env-file",
        default=None,
        help="Path to .env file (default: .env in working dir or system env vars)",
    )
    args = parser.parse_args()

    asyncio.run(
        async_main(
            db_type=args.db_type,
            json_report=args.json_report,
            md_report=args.md_report,
            entities_config=args.entities_config,
            env_file=args.env_file,
        )
    )


if __name__ == "__main__":
    main()
