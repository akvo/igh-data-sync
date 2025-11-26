#!/usr/bin/env python3
"""
Dataverse to SQLite Sync

Syncs Dataverse entities to SQLite with integrated schema validation.
Uses authoritative $metadata schemas for type-accurate table creation.

Usage:
    python sync_dataverse.py

Exit codes:
    0 - Sync completed successfully
    1 - Sync failed (schema validation or sync errors)
"""
import sys
import asyncio
from lib.config import load_config, load_entity_configs
from lib.auth import DataverseAuth
from lib.dataverse_client import DataverseClient
from lib.validation.dataverse_schema import DataverseSchemaFetcher
from lib.validation.database_schema import DatabaseSchemaQuery
from lib.validation.schema_comparer import SchemaComparer
from lib.sync.database import DatabaseManager
from lib.sync.schema_initializer import initialize_tables
from lib.sync.sync_state import SyncStateManager


async def validate_schema_before_sync(config, entities, client, db_manager):
    """
    Validate schema against Dataverse $metadata.

    Returns: (valid_entities, entities_to_create, differences)
    """
    print("  Fetching schemas from Dataverse $metadata...")

    fetcher = DataverseSchemaFetcher(client, target_db='sqlite')
    dv_schemas = await fetcher.fetch_schemas_from_metadata([e.name for e in entities])

    # Query database schemas
    db_query = DatabaseSchemaQuery(config, db_type='sqlite')
    db_schemas = db_query.query_all_schemas([e.api_name for e in entities])

    # Compare schemas
    comparer = SchemaComparer(target_db='sqlite')
    differences = []
    valid_entities = []
    entities_to_create = []
    entity_map = {e.name: e for e in entities}

    for entity in entities:
        singular_name = entity.name
        plural_name = entity.api_name

        # Check if in Dataverse
        if singular_name not in dv_schemas:
            differences.append({
                'entity': plural_name,
                'severity': 'warning',
                'description': f"Entity '{singular_name}' in config but not in $metadata - skipping"
            })
            continue

        dv_schema = dv_schemas[singular_name]

        # Check if table exists
        if plural_name not in db_schemas:
            differences.append({
                'entity': plural_name,
                'severity': 'info',
                'description': f"New entity - table will be created"
            })
            entities_to_create.append(entity)
            valid_entities.append(entity)
        else:
            # Compare schemas
            db_schema = db_schemas[plural_name]
            entity_diffs = comparer.compare_all({singular_name: dv_schema}, {plural_name: db_schema})
            for diff in entity_diffs:
                differences.append({
                    'entity': diff.entity,
                    'severity': diff.severity,
                    'description': diff.description,
                    'details': diff.details
                })
            valid_entities.append(entity)

    # Analyze
    errors = [d for d in differences if d['severity'] == 'error']
    warnings = [d for d in differences if d['severity'] == 'warning']
    infos = [d for d in differences if d['severity'] == 'info']

    if differences:
        print(f"\n  Schema Validation Results:")
        print(f"    Errors: {len(errors)}, Warnings: {len(warnings)}, Info: {len(infos)}\n")

        for diff in errors:
            print(f"    ❌ ERROR [{diff['entity']}]: {diff['description']}")
        for diff in warnings:
            print(f"    ⚠️  WARNING [{diff['entity']}]: {diff['description']}")
        for diff in infos:
            print(f"    ℹ️  INFO [{diff['entity']}]: {diff['description']}")

    # Exit if errors
    if errors:
        print(f"\n❌ SYNC ABORTED: {len(errors)} breaking schema change(s)")
        sys.exit(1)

    if warnings or infos:
        print(f"\n  ✓ Validation passed with {len(warnings)} warning(s), {len(infos)} info")
    else:
        print(f"\n  ✓ Validation passed (no changes)")

    return valid_entities, entities_to_create, differences


async def sync_entity(entity, client, db_manager, state_manager, dv_schemas):
    """Sync single entity."""
    try:
        log_id = state_manager.start_sync(entity.api_name)
        print(f"  Syncing {entity.api_name}...")

        # Get schema
        schema = dv_schemas[entity.name]

        # Get last timestamp
        last_timestamp = db_manager.get_last_sync_timestamp(entity.api_name)

        # Build filter
        filter_query = None
        if last_timestamp and 'modifiedon' in [c.name for c in schema.columns]:
            filter_query = f"modifiedon gt {last_timestamp}"

        # Determine orderby
        orderby = None
        if schema.primary_key:
            orderby = schema.primary_key
        elif 'createdon' in [c.name for c in schema.columns]:
            orderby = 'createdon'
        elif 'modifiedon' in [c.name for c in schema.columns]:
            orderby = 'modifiedon'

        # Fetch all pages
        records = await client.fetch_all_pages(
            entity.api_name,
            orderby=orderby,
            filter_query=filter_query
        )

        if not records:
            state_manager.complete_sync(log_id, entity.api_name, 0, 0)
            print(f"  ✓ {entity.api_name}: No records")
            return 0, 0

        # Determine actual primary key to use for UPSERT
        # Handle case where $metadata primary_key doesn't exist in actual columns
        actual_pk = schema.primary_key
        column_names = [c.name for c in schema.columns]

        if actual_pk and actual_pk not in column_names:
            # Primary key from metadata doesn't exist in columns (e.g., systemuser's ownerid)
            # Try to find alternative: {entity_name}id
            fallback_pk = f"{entity.name}id"
            if fallback_pk in column_names:
                actual_pk = fallback_pk
                print(f"    ⚠️  Primary key '{schema.primary_key}' not in columns, using '{actual_pk}' instead")
            elif fallback_pk in records[0]:
                # It's in the API response but not in schema columns - add it
                actual_pk = fallback_pk
                print(f"    ⚠️  Primary key '{schema.primary_key}' not in columns, using '{actual_pk}' from API response")
            else:
                # Last resort: find any column ending with 'id' that exists in both schema and data
                id_cols = [name for name in column_names if name.endswith('id') and not name.startswith('_')]
                if id_cols:
                    actual_pk = id_cols[0]
                    print(f"    ⚠️  Primary key '{schema.primary_key}' not in columns, using '{actual_pk}' instead")
                else:
                    raise RuntimeError(f"Cannot find valid primary key for {entity.api_name}")

        # Upsert
        added, updated = db_manager.upsert_batch(
            entity.api_name,
            actual_pk,
            schema,
            records
        )

        # Update timestamp
        if records:
            # Get all non-null modifiedon values
            timestamps = [r['modifiedon'] for r in records if r.get('modifiedon')]
            print(f"    DEBUG: Found {len(timestamps)} records with modifiedon out of {len(records)} total")
            if timestamps:
                max_timestamp = max(timestamps)
                print(f"    DEBUG: Saving timestamp {max_timestamp}")
                db_manager.update_sync_timestamp(entity.api_name, max_timestamp, len(records))
            else:
                print(f"    DEBUG: No timestamps found, not saving")

        state_manager.complete_sync(log_id, entity.api_name, added, updated)
        print(f"  ✓ {entity.api_name}: {added} added, {updated} updated")

        return added, updated

    except Exception as e:
        state_manager.fail_sync(log_id, entity.api_name, str(e))
        print(f"  ❌ {entity.api_name}: Failed - {e}")
        raise


async def main():
    """Main sync workflow."""
    print("=" * 60)
    print("DATAVERSE TO SQLITE SYNC")
    print("=" * 60)

    try:
        # [1/7] Load Configuration
        print("\n[1/7] Loading configuration...")
        config = load_config()
        entities = load_entity_configs()
        print(f"  ✓ Loaded config for {len(entities)} entities")
        print(f"  ✓ Database: {config.sqlite_db_path}")

        # [2/7] Authenticate
        print("\n[2/7] Authenticating...")
        auth = DataverseAuth(config)
        token = auth.get_token()
        print(f"  ✓ Authenticated (tenant: {auth.tenant_id})")

        # [3/7] Validate Schema
        print("\n[3/7] Validating schema...")
        async with DataverseClient(config, token) as client:
            db_manager = DatabaseManager(config.sqlite_db_path)

            valid_entities, entities_to_create, diffs = await validate_schema_before_sync(
                config, entities, client, db_manager
            )

            if not valid_entities:
                print("\n❌ No valid entities to sync")
                sys.exit(1)

            # [4/7] Initialize Database
            print("\n[4/7] Initializing database...")
            db_manager.init_sync_tables()
            print("  ✓ Sync tables initialized")

            if entities_to_create:
                await initialize_tables(config, entities_to_create, client, db_manager)

            # [5/7] Fetch schemas for sync
            print("\n[5/7] Preparing for sync...")
            fetcher = DataverseSchemaFetcher(client, target_db='sqlite')
            dv_schemas = await fetcher.fetch_schemas_from_metadata([e.name for e in valid_entities])
            print(f"  ✓ Schemas loaded for {len(dv_schemas)} entities")

            # [6/7] Sync Entities
            print("\n[6/7] Syncing data...")
            state_manager = SyncStateManager(db_manager)

            # Separate filtered and unfiltered
            unfiltered = [e for e in valid_entities if not e.filtered]
            filtered = [e for e in valid_entities if e.filtered]

            # Sync unfiltered entities
            print(f"\n  Syncing {len(unfiltered)} unfiltered entities...")
            total_added = 0
            total_updated = 0
            failed_entities = []

            for entity in unfiltered:
                if entity.name not in dv_schemas:
                    continue
                try:
                    added, updated = await sync_entity(entity, client, db_manager, state_manager, dv_schemas)
                    total_added += added
                    total_updated += updated
                except Exception as e:
                    # Log error but continue syncing other entities
                    failed_entities.append((entity.api_name, str(e)))
                    # sync_entity already printed error and called fail_sync
                    continue

            # Sync filtered entities (simplified for now)
            if filtered:
                print(f"\n  Syncing {len(filtered)} filtered entities...")
                for entity in filtered:
                    if entity.name not in dv_schemas:
                        continue
                    try:
                        added, updated = await sync_entity(entity, client, db_manager, state_manager, dv_schemas)
                        total_added += added
                        total_updated += updated
                    except Exception as e:
                        # Log error but continue syncing other entities
                        failed_entities.append((entity.api_name, str(e)))
                        # sync_entity already printed error and called fail_sync
                        continue

            # Report any failures
            if failed_entities:
                print(f"\n⚠️  {len(failed_entities)} entity/entities failed to sync:")
                for entity_name, error in failed_entities:
                    error_preview = error[:100] + "..." if len(error) > 100 else error
                    print(f"  - {entity_name}: {error_preview}")

            db_manager.close()

        # [7/7] Summary
        print("\n[7/7] Sync complete!")
        print("=" * 60)
        print(f"Total records added: {total_added}")
        print(f"Total records updated: {total_updated}")
        print("=" * 60)

        sys.exit(0)

    except KeyboardInterrupt:
        print("\n\nInterrupted by user")
        sys.exit(1)
    except Exception as e:
        print(f"\n❌ SYNC FAILED: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)


if __name__ == '__main__':
    asyncio.run(main())
