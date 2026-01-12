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
import logging
import sys
import traceback
from typing import Optional

from igh_data_sync.auth import DataverseAuth
from igh_data_sync.config import Config, EntityConfig, load_config, load_entity_configs, load_optionsets_config
from igh_data_sync.dataverse_client import DataverseClient
from igh_data_sync.sync.database import DatabaseManager
from igh_data_sync.sync.entity_sync import sync_entity
from igh_data_sync.sync.filtered_sync import FilteredSyncManager
from igh_data_sync.sync.reference_verifier import ReferenceVerifier
from igh_data_sync.sync.relationship_graph import RelationshipGraph
from igh_data_sync.sync.schema_initializer import initialize_tables
from igh_data_sync.sync.sync_state import SyncStateManager
from igh_data_sync.validation.dataverse_schema import DataverseSchemaFetcher
from igh_data_sync.validation.validator import validate_schema_before_sync

# Maximum length of error message to display in failure report
MAX_ERROR_MESSAGE_LENGTH = 100


def _log(message: str, logger: Optional[logging.Logger] = None):
    """Log message using logger if provided, otherwise print."""
    if logger:
        logger.info(message)
    else:
        print(message)


def _load_configuration(env_file=None, entities_config=None, logger=None):
    """Load configuration and entity configs."""
    _log("\n[1/7] Loading configuration...", logger)
    config = load_config(env_file=env_file)
    entities = load_entity_configs(path=entities_config)
    _log(f"  ✓ Loaded config for {len(entities)} entities", logger)
    _log(f"  ✓ Database: {config.sqlite_db_path}", logger)
    return config, entities


def _authenticate(config, logger=None):
    """Authenticate with Dataverse."""
    _log("\n[2/7] Authenticating...", logger)
    auth = DataverseAuth(config)
    token = auth.get_token()
    _log(f"  ✓ Authenticated (tenant: {auth.tenant_id})", logger)
    return token


async def _initialize_database(config, entities_to_create, client, db_manager, option_set_fields_by_entity=None, logger=None):
    """Initialize database tables."""
    _log("\n[4/7] Initializing database...", logger)
    db_manager.init_sync_tables()
    _log("  ✓ Sync tables initialized", logger)

    if entities_to_create:
        await initialize_tables(config, entities_to_create, client, db_manager, option_set_fields_by_entity)


async def _prepare_sync(client, valid_entities, logger=None):
    """Fetch schemas and build relationship graph."""
    _log("\n[5/7] Preparing for sync...", logger)
    fetcher = DataverseSchemaFetcher(client, target_db="sqlite")
    dv_schemas = await fetcher.fetch_schemas_from_metadata([e.name for e in valid_entities])
    _log(f"  ✓ Schemas loaded for {len(dv_schemas)} entities", logger)
    return fetcher, dv_schemas


async def _build_relationship_graph(fetcher, entities, logger=None):
    """Build relationship graph for filtered sync."""
    _log("\n[5.5/7] Building relationship graph...", logger)
    metadata_xml = await fetcher.fetch_metadata_xml()
    relationship_graph = RelationshipGraph.build_from_metadata(metadata_xml, entities)
    _log("  ✓ Relationship graph built", logger)
    return relationship_graph


async def _sync_unfiltered_entities(unfiltered, dv_schemas, client, db_manager, state_manager, logger=None):
    """Sync unfiltered entities."""
    _log(f"\n  Syncing {len(unfiltered)} unfiltered entities...", logger)
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
    logger=None,
):
    """Sync filtered entities using transitive closure."""
    _log(f"\n  Syncing {len(filtered)} filtered entities with transitive closure...", logger)
    sync_manager = FilteredSyncManager(client, db_manager, state_manager)

    # Track synced IDs to detect convergence
    synced_ids = {entity.api_name: set() for entity in filtered}
    max_iterations = 5
    total_added = 0
    total_updated = 0
    failed_entities = []

    for iteration in range(1, max_iterations + 1):
        _log(f"\n  Transitive closure iteration {iteration}:", logger)

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
                _log(f"    {entity.api_name}: {len(new_ids)} new IDs to sync", logger)

        # If no new IDs, we've converged
        if not has_new_ids:
            _log("    Converged - no new IDs found", logger)
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
                _log(f"    ✓ {entity.api_name}: {added} added, {updated} updated", logger)
            except Exception as e:
                # Log error but continue syncing other entities
                failed_entities.append((entity.api_name, str(e)))
                _log(f"    ❌ {entity.api_name}: Failed - {e}", logger)
                continue

    # Log final statistics
    _log("\n  Filtered entity sync complete:", logger)
    for entity in filtered:
        count = len(synced_ids[entity.api_name])
        _log(f"    {entity.api_name}: {count} total records synced", logger)

    return total_added, total_updated, failed_entities


def _report_failures(failed_entities, logger=None):
    """Report any sync failures."""
    if failed_entities:
        _log(f"\n⚠️  {len(failed_entities)} entity/entities failed to sync:", logger)
        for entity_name, error in failed_entities:
            error_preview = error[:MAX_ERROR_MESSAGE_LENGTH] + "..." if len(error) > MAX_ERROR_MESSAGE_LENGTH else error
            _log(f"  - {entity_name}: {error_preview}", logger)


def _verify_references(verify_references, db_manager, relationship_graph, logger=None):
    """
    Verify reference integrity if requested.

    Returns:
        tuple: (has_issues, issues_list) where has_issues is bool and issues_list is list of issues
    """
    if not verify_references:
        return False, []

    _log("\n[7/7] Verifying references...", logger)
    verifier = ReferenceVerifier()
    report = verifier.verify_references(db_manager, relationship_graph)
    _log(str(report), logger)

    # Return issues instead of calling sys.exit()
    return bool(report.issues), report.issues


def _print_summary(total_added, total_updated, logger=None):
    """Print sync summary."""
    _log("\n[8/8] Sync complete!", logger)
    _log("=" * 60, logger)
    _log(f"Total records added: {total_added}", logger)
    _log(f"Total records updated: {total_updated}", logger)
    _log("=" * 60, logger)


async def run_sync_workflow(client, config, entities, db_manager, verify_references=False, option_set_fields_by_entity=None, logger=None):
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
        option_set_fields_by_entity: Optional dict mapping entity names to option set field names
        logger: Optional logger for output (if None, uses print)

    Returns:
        dict: Sync results with keys:
            - success (bool): True if sync completed without errors
            - total_added (int): Number of records added
            - total_updated (int): Number of records updated
            - failed_entities (list): List of (entity_name, error_message) tuples
            - validation_errors (list): List of validation errors if validation failed
            - reference_errors (list): List of reference integrity issues if verify_references=True
    """
    # Validate schema
    _log("\n[3/7] Validating schema...", logger)
    valid_entities, entities_to_create, differences, validation_passed = await validate_schema_before_sync(
        config,
        entities,
        client,
        db_manager,
        logger,
    )

    # Check validation results
    if not validation_passed:
        validation_errors = [d for d in differences if d["severity"] == "error"]
        return {
            "success": False,
            "total_added": 0,
            "total_updated": 0,
            "failed_entities": [],
            "validation_errors": validation_errors,
            "reference_errors": [],
        }

    if not valid_entities:
        _log("\n❌ No valid entities to sync", logger)
        return {
            "success": False,
            "total_added": 0,
            "total_updated": 0,
            "failed_entities": [],
            "validation_errors": [],
            "reference_errors": [],
        }

    # Initialize database and prepare
    await _initialize_database(config, entities_to_create, client, db_manager, option_set_fields_by_entity, logger)
    fetcher, dv_schemas = await _prepare_sync(client, valid_entities, logger)
    relationship_graph = await _build_relationship_graph(fetcher, entities, logger)

    # Sync entities
    _log("\n[6/7] Syncing data...", logger)
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
        logger,
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
            logger,
        )
        total_added += f_added
        total_updated += f_updated
        failed_entities.extend(f_failed)

    # Report and verify
    _report_failures(failed_entities, logger)
    has_reference_issues, reference_issues = _verify_references(verify_references, db_manager, relationship_graph, logger)

    # Summary
    _print_summary(total_added, total_updated, logger)

    # Determine overall success
    success = len(failed_entities) == 0 and not has_reference_issues

    return {
        "success": success,
        "total_added": total_added,
        "total_updated": total_updated,
        "failed_entities": failed_entities,
        "validation_errors": [],
        "reference_errors": reference_issues,
    }


async def run_sync(
    config: Config,
    entities_config: Optional[list[EntityConfig]] = None,
    optionsets_config: Optional[dict] = None,
    verify_reference: bool = False,
    logger: Optional[logging.Logger] = None,
) -> bool:
    """
    Programmatic async entry point for Dataverse sync.

    Designed for use from Apache Airflow or other orchestration tools.
    Handles authentication, database connection, and complete sync workflow.

    Args:
        config: Configuration object with Dataverse API credentials and database settings.
                Required fields: api_url, client_id, client_secret, scope, sqlite_db_path
        entities_config: List of EntityConfig objects to sync.
                        If None, loads from default package data (entities_config.json)
        optionsets_config: Dict mapping entity names to option set field lists.
                          If None, loads from default package data (optionsets.json).
                          Currently unused by core logic but included for future use.
        verify_reference: If True, verify foreign key reference integrity after sync.
                         Sync will fail (return False) if dangling references found.
        logger: Optional Python logger for output. If None, runs silently.

    Returns:
        bool: True if sync completed successfully, False if any errors occurred
              (validation failures, sync errors, or reference integrity issues)

    Raises:
        ValueError: If config is missing required fields
        RuntimeError: If authentication fails

    Example:
        ```python
        from igh_data_sync import run_sync
        from igh_data_sync.config import Config
        import logging

        config = Config(
            api_url="https://org.api.crm.dynamics.com/api/data/v9.2/",
            client_id="...",
            client_secret="...",
            scope="...",
            sqlite_db_path="dataverse.db"
        )

        logger = logging.getLogger(__name__)
        success = await run_sync(config, verify_reference=True, logger=logger)

        if not success:
            raise Exception("Sync failed")
        ```
    """
    # [1] Load entities config if not provided
    if entities_config is None:
        if logger:
            logger.info("Loading entities from package defaults")
        entities_config = load_entity_configs()  # Uses package data

    # [2] Load optionsets config if not provided
    if optionsets_config is None:
        if logger:
            logger.info("Loading optionsets from package defaults")
        optionsets_config = load_optionsets_config()  # Uses package data

    # [3] Authenticate with Dataverse
    if logger:
        logger.info("Authenticating with Dataverse")
    try:
        auth = DataverseAuth(config)
        token = auth.get_token()
    except Exception as e:
        if logger:
            logger.error(f"Authentication failed: {e}")
        raise RuntimeError(f"Authentication failed: {e}") from e

    # [4] Run sync workflow with context managers
    if logger:
        logger.info("Starting sync workflow")

    try:
        async with DataverseClient(config, token) as client:
            with DatabaseManager(config.sqlite_db_path) as db_manager:
                results = await run_sync_workflow(
                    client=client,
                    config=config,
                    entities=entities_config,
                    db_manager=db_manager,
                    verify_references=verify_reference,
                    option_set_fields_by_entity=optionsets_config,
                    logger=logger,
                )

        # [5] Determine success from results
        success = results["success"]

        if logger:
            if success:
                logger.info(
                    f"Sync completed successfully: "
                    f"+{results['total_added']} added, "
                    f"{results['total_updated']} updated"
                )
            else:
                logger.error(
                    f"Sync failed: "
                    f"{len(results['failed_entities'])} entity failures, "
                    f"{len(results.get('validation_errors', []))} validation errors, "
                    f"{len(results.get('reference_errors', []))} reference errors"
                )

        return success

    except Exception:
        if logger:
            logger.exception("Sync workflow failed with exception")
        raise


async def async_main(verify_references=False, env_file=None, entities_config=None, optionsets_config=None):
    """
    CLI entry point - wraps run_sync() with CLI-specific concerns.

    Handles:
    - Loading configuration from files/environment
    - Setting up console output (print statements)
    - Exit codes based on success/failure

    Args:
        verify_references: If True, verify reference integrity after sync
        env_file: Path to .env file (default: .env in working dir or system env vars)
        entities_config: Path to entities config file (default: package data)
        optionsets_config: Path to optionsets config file (default: package data)
    """
    print("=" * 60)
    print("DATAVERSE TO SQLITE SYNC")
    print("=" * 60)

    try:
        # [1] Load configuration
        print("\n[1/2] Loading configuration...")
        config = load_config(env_file=env_file)

        # Load entities if custom path provided
        entities = None
        if entities_config:
            entities = load_entity_configs(path=entities_config)

        # Load optionsets if custom path provided
        optionsets = None
        if optionsets_config:
            optionsets = load_optionsets_config(path=optionsets_config)

        print("✓ Configuration loaded")

        # [2] Run sync (with console output)
        print("\n[2/2] Running sync workflow...")

        # Create a simple logger that prints to console
        console_logger = logging.getLogger("sync")
        console_logger.setLevel(logging.INFO)
        if not console_logger.handlers:
            handler = logging.StreamHandler()
            handler.setFormatter(logging.Formatter("%(message)s"))
            console_logger.addHandler(handler)

        success = await run_sync(
            config=config,
            entities_config=entities,
            optionsets_config=optionsets,
            verify_reference=verify_references,
            logger=console_logger,
        )

        if success:
            print("\n✓ Sync completed successfully")
            sys.exit(0)
        else:
            print("\n❌ Sync failed - check logs for details")
            sys.exit(1)

    except KeyboardInterrupt:
        print("\n\nInterrupted by user")
        sys.exit(1)
    except Exception as e:
        print(f"\n❌ SYNC FAILED: {e}")
        traceback.print_exc()
        sys.exit(1)


def main():
    """CLI entry point for sync-dataverse command."""
    parser = argparse.ArgumentParser(description="Sync Dataverse entities to SQLite database")
    parser.add_argument(
        "--verify",
        action="store_true",
        help="Verify reference integrity after sync (exits with error if dangling references found)",
    )
    parser.add_argument(
        "--entities-config",
        help="Path to entities config file (default: package data)",
    )
    parser.add_argument(
        "--optionsets-config",
        help="Path to optionsets config file (default: package data)",
    )
    parser.add_argument(
        "--env-file",
        help="Path to .env file (default: .env in working dir or system env vars)",
    )
    args = parser.parse_args()

    asyncio.run(
        async_main(
            verify_references=args.verify,
            env_file=args.env_file,
            entities_config=args.entities_config,
            optionsets_config=args.optionsets_config,
        )
    )


if __name__ == "__main__":
    main()
