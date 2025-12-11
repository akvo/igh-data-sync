#!/usr/bin/env python3
"""
Dataverse to SQLite Sync

Syncs Dataverse entities to SQLite with integrated schema validation.
Uses authoritative $metadata schemas for type-accurate table creation.
Implements filtered entity sync with transitive closure for minimal data transfer.

Usage:
    python sync_dataverse.py           # Normal sync
    python sync_dataverse.py --verify  # Sync + verify reference integrity

Exit codes:
    0 - Sync completed successfully
    1 - Sync failed (schema validation, sync errors, or dangling references with --verify)
"""

import argparse
import asyncio
import sys
import traceback

from lib.auth import DataverseAuth
from lib.config import load_config, load_entity_configs
from lib.dataverse_client import DataverseClient
from lib.sync.database import DatabaseManager
from lib.sync.entity_sync import sync_entity
from lib.sync.filtered_sync import FilteredSyncManager
from lib.sync.reference_verifier import ReferenceVerifier
from lib.sync.relationship_graph import RelationshipGraph
from lib.sync.schema_initializer import initialize_tables
from lib.sync.sync_state import SyncStateManager
from lib.validation.dataverse_schema import DataverseSchemaFetcher
from lib.validation.validator import validate_schema_before_sync

# Maximum length of error message to display in failure report
MAX_ERROR_MESSAGE_LENGTH = 100


def _load_configuration():
    """Load configuration and entity configs."""
    print("\n[1/7] Loading configuration...")
    config = load_config()
    entities = load_entity_configs()
    print(f"  ✓ Loaded config for {len(entities)} entities")
    print(f"  ✓ Database: {config.sqlite_db_path}")
    return config, entities


def _authenticate(config):
    """Authenticate with Dataverse."""
    print("\n[2/7] Authenticating...")
    auth = DataverseAuth(config)
    token = auth.get_token()
    print(f"  ✓ Authenticated (tenant: {auth.tenant_id})")
    return token


async def _initialize_database(config, entities_to_create, client, db_manager):
    """Initialize database tables."""
    print("\n[4/7] Initializing database...")
    db_manager.init_sync_tables()
    print("  ✓ Sync tables initialized")

    if entities_to_create:
        await initialize_tables(config, entities_to_create, client, db_manager)


async def _prepare_sync(client, valid_entities):
    """Fetch schemas and build relationship graph."""
    print("\n[5/7] Preparing for sync...")
    fetcher = DataverseSchemaFetcher(client, target_db="sqlite")
    dv_schemas = await fetcher.fetch_schemas_from_metadata([e.name for e in valid_entities])
    print(f"  ✓ Schemas loaded for {len(dv_schemas)} entities")
    return fetcher, dv_schemas


async def _build_relationship_graph(fetcher, entities):
    """Build relationship graph for filtered sync."""
    print("\n[5.5/7] Building relationship graph...")
    metadata_xml = await fetcher.fetch_metadata_xml()
    relationship_graph = RelationshipGraph.build_from_metadata(metadata_xml, entities)
    print("  ✓ Relationship graph built")
    return relationship_graph


async def _sync_unfiltered_entities(unfiltered, dv_schemas, client, db_manager, state_manager):
    """Sync unfiltered entities."""
    print(f"\n  Syncing {len(unfiltered)} unfiltered entities...")
    total_added = 0
    total_updated = 0
    failed_entities = []

    for entity in unfiltered:
        if entity.name not in dv_schemas:
            continue
        try:
            added, updated = await sync_entity(
                entity,
                client,
                db_manager,
                state_manager,
                dv_schemas,
            )
            total_added += added
            total_updated += updated
        except Exception as e:
            # Log error but continue syncing other entities
            failed_entities.append((entity.api_name, str(e)))
            # sync_entity already printed error and called fail_sync
            continue

    return total_added, total_updated, failed_entities


async def _sync_filtered_entities(
    filtered,
    dv_schemas,
    client,
    db_manager,
    state_manager,
    relationship_graph,
):
    """Sync filtered entities using transitive closure."""
    print(f"\n  Syncing {len(filtered)} filtered entities with transitive closure...")
    sync_manager = FilteredSyncManager(client, db_manager, state_manager)

    # Track synced IDs to detect convergence
    synced_ids = {entity.api_name: set() for entity in filtered}
    max_iterations = 5
    total_added = 0
    total_updated = 0
    failed_entities = []

    for iteration in range(1, max_iterations + 1):
        print(f"\n  Transitive closure iteration {iteration}:")

        # Extract IDs based on current database state
        filtered_ids = FilteredSyncManager.extract_filtered_ids(
            relationship_graph,
            db_manager,
            [e.api_name for e in filtered],
        )

        # Check for new IDs
        has_new_ids = False
        for entity in filtered:
            new_ids = filtered_ids.get(entity.api_name, set()) - synced_ids[entity.api_name]
            if new_ids:
                has_new_ids = True
                print(f"    {entity.api_name}: {len(new_ids)} new IDs to sync")

        # If no new IDs, we've converged
        if not has_new_ids:
            print("    Converged - no new IDs found")
            break

        # Sync entities with new IDs
        for entity in filtered:
            if entity.name not in dv_schemas:
                continue

            new_ids = filtered_ids.get(entity.api_name, set()) - synced_ids[entity.api_name]
            if not new_ids:
                continue

            try:
                added, updated = await sync_manager.sync_filtered_entity(
                    entity,
                    new_ids,
                    dv_schemas[entity.name],
                )
                total_added += added
                total_updated += updated
                synced_ids[entity.api_name].update(new_ids)
                print(f"    ✓ {entity.api_name}: {added} added, {updated} updated")
            except Exception as e:
                # Log error but continue syncing other entities
                failed_entities.append((entity.api_name, str(e)))
                print(f"    ❌ {entity.api_name}: Failed - {e}")
                continue

    # Log final statistics
    print("\n  Filtered entity sync complete:")
    for entity in filtered:
        count = len(synced_ids[entity.api_name])
        print(f"    {entity.api_name}: {count} total records synced")

    return total_added, total_updated, failed_entities


def _report_failures(failed_entities):
    """Report any sync failures."""
    if failed_entities:
        print(f"\n⚠️  {len(failed_entities)} entity/entities failed to sync:")
        for entity_name, error in failed_entities:
            error_preview = error[:MAX_ERROR_MESSAGE_LENGTH] + "..." if len(error) > MAX_ERROR_MESSAGE_LENGTH else error
            print(f"  - {entity_name}: {error_preview}")


def _verify_references_if_needed(verify_references, db_manager, relationship_graph):
    """Verify reference integrity if requested."""
    if verify_references:
        print("\n[7/7] Verifying references...")
        verifier = ReferenceVerifier()
        report = verifier.verify_references(db_manager, relationship_graph)
        print(report)

        # Exit with error if issues found
        if report.issues:
            db_manager.close()
            sys.exit(1)


def _print_summary(total_added, total_updated):
    """Print sync summary."""
    print("\n[8/8] Sync complete!")
    print("=" * 60)
    print(f"Total records added: {total_added}")
    print(f"Total records updated: {total_updated}")
    print("=" * 60)


async def run_sync_workflow(client, config, entities, db_manager, verify_references=False):
    """
    Core sync workflow - extracted for testability.

    This function contains all the business logic for syncing entities from Dataverse
    to SQLite. It can be called directly from tests with a fake client.

    Args:
        client: DataverseClient (real or fake for testing)
        config: Configuration object
        entities: List of EntityConfig objects to sync
        db_manager: DatabaseManager instance
        verify_references: If True, verify reference integrity after sync
    """
    # Validate schema
    print("\n[3/7] Validating schema...")
    valid_entities, entities_to_create, _diffs = await validate_schema_before_sync(
        config,
        entities,
        client,
        db_manager,
    )

    if not valid_entities:
        print("\n❌ No valid entities to sync")
        return

    # Initialize database and prepare
    await _initialize_database(config, entities_to_create, client, db_manager)
    fetcher, dv_schemas = await _prepare_sync(client, valid_entities)
    relationship_graph = await _build_relationship_graph(fetcher, entities)

    # Sync entities
    print("\n[6/7] Syncing data...")
    state_manager = SyncStateManager(db_manager)
    unfiltered = [e for e in valid_entities if not e.filtered]
    filtered = [e for e in valid_entities if e.filtered]

    # Sync unfiltered
    total_added, total_updated, failed_entities = await _sync_unfiltered_entities(
        unfiltered,
        dv_schemas,
        client,
        db_manager,
        state_manager,
    )

    # Sync filtered
    if filtered:
        f_added, f_updated, f_failed = await _sync_filtered_entities(
            filtered,
            dv_schemas,
            client,
            db_manager,
            state_manager,
            relationship_graph,
        )
        total_added += f_added
        total_updated += f_updated
        failed_entities.extend(f_failed)

    # Report and verify
    _report_failures(failed_entities)
    _verify_references_if_needed(verify_references, db_manager, relationship_graph)
    db_manager.close()

    # Summary
    _print_summary(total_added, total_updated)


async def main(verify_references=False):
    """
    Main entry point - thin shell for configuration and authentication.

    This function handles:
    - Loading configuration from environment
    - OAuth authentication with Azure
    - Creating DataverseClient with context manager
    - Delegating to run_sync_workflow() for actual sync logic

    Args:
        verify_references: If True, verify reference integrity after sync
    """
    print("=" * 60)
    print("DATAVERSE TO SQLITE SYNC")
    print("=" * 60)

    try:
        # [1-2] Load config and authenticate
        config, entities = _load_configuration()
        token = _authenticate(config)

        # [3-8] Run sync workflow
        async with DataverseClient(config, token) as client:
            db_manager = DatabaseManager(config.sqlite_db_path)
            await run_sync_workflow(client, config, entities, db_manager, verify_references)

        sys.exit(0)

    except KeyboardInterrupt:
        print("\n\nInterrupted by user")
        sys.exit(1)
    except Exception as e:
        print(f"\n❌ SYNC FAILED: {e}")
        traceback.print_exc()
        sys.exit(1)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Sync Dataverse entities to SQLite database")
    parser.add_argument(
        "--verify",
        action="store_true",
        help=("Verify reference integrity after sync (exits with error if dangling references found)"),
    )
    args = parser.parse_args()

    asyncio.run(main(verify_references=args.verify))
