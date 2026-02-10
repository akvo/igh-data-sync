"""
Helper functions for the Dataverse sync workflow.

These functions implement the individual steps of the sync process and are called
from the main workflow in sync.py. Extracting them here keeps the main sync.py
file focused on the high-level workflow logic.
"""

import logging
from typing import Optional

from igh_data_sync.sync.entity_sync import sync_entity
from igh_data_sync.sync.filtered_sync import FilteredSyncManager
from igh_data_sync.sync.reference_verifier import ReferenceVerifier
from igh_data_sync.sync.relationship_graph import RelationshipGraph
from igh_data_sync.sync.schema_initializer import initialize_tables
from igh_data_sync.validation.dataverse_schema import DataverseSchemaFetcher

# Maximum length of error message to display in failure report
MAX_ERROR_MESSAGE_LENGTH = 100


def _log(message: str, logger: Optional[logging.Logger] = None):
    """Log message using logger if provided, otherwise print."""
    if logger:
        logger.info(message)
    else:
        print(message)


async def _initialize_database(
    config, entities_to_create, client, db_manager, option_set_fields_by_entity=None, logger=None
):
    """Initialize database tables."""
    _log("\n[4/7] Initializing database...", logger)
    db_manager.init_sync_tables()
    _log("  \u2713 Sync tables initialized", logger)

    if entities_to_create:
        await initialize_tables(config, entities_to_create, client, db_manager, option_set_fields_by_entity)


async def _prepare_sync(client, valid_entities, logger=None):
    """Fetch schemas and build relationship graph."""
    _log("\n[5/7] Preparing for sync...", logger)
    fetcher = DataverseSchemaFetcher(client, target_db="sqlite")
    dv_schemas = await fetcher.fetch_schemas_from_metadata([e.name for e in valid_entities])
    _log(f"  \u2713 Schemas loaded for {len(dv_schemas)} entities", logger)
    return fetcher, dv_schemas


async def _build_relationship_graph(fetcher, entities, logger=None):
    """Build relationship graph for filtered sync."""
    _log("\n[5.5/7] Building relationship graph...", logger)
    metadata_xml = await fetcher.fetch_metadata_xml()
    relationship_graph = RelationshipGraph.build_from_metadata(metadata_xml, entities)
    _log("  \u2713 Relationship graph built", logger)
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


async def _sync_filtered_entities(  # noqa: PLR0913, PLR0917
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
                _log(f"    \u2713 {entity.api_name}: {added} added, {updated} updated", logger)
            except Exception as e:
                # Log error but continue syncing other entities
                failed_entities.append((entity.api_name, str(e)))
                _log(f"    \u274c {entity.api_name}: Failed - {e}", logger)
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
        _log(f"\n\u26a0\ufe0f  {len(failed_entities)} entity/entities failed to sync:", logger)
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
