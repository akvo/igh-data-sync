"""Filtered entity sync manager with transitive closure ID extraction.

Syncs only referenced records for filtered entities (accounts, contacts, systemusers)
instead of downloading all records from Dataverse.
"""

from ..config import EntityConfig
from ..dataverse_client import DataverseClient
from ..type_mapping import TableSchema
from .database import DatabaseManager
from .relationship_graph import RelationshipGraph
from .sync_state import SyncStateManager


class FilteredSyncManager:
    """
    Manages filtered entity synchronization using transitive closure ID extraction.
    """

    MAX_ITERATIONS = 10  # Safety limit to prevent infinite loops
    BATCH_SIZE = 50  # Max IDs per $filter query (avoid URL length limits)

    @staticmethod
    def extract_filtered_ids(
        relationship_graph: RelationshipGraph,
        db_manager: DatabaseManager,
        filtered_entities: list[str],
    ) -> dict[str, set[str]]:
        """
        Extract IDs for filtered entities using transitive closure.

        Iteratively queries the database to find all referenced IDs until
        no new IDs are discovered (convergence).

        Args:
            relationship_graph: Graph of entity relationships
            db_manager: Database manager for querying
            filtered_entities: List of entity API names to extract IDs for

        Returns:
            Dict mapping entity_api_name → Set of IDs to sync

        Algorithm:
            1. Initialize result = {entity: set() for entity in filtered_entities}
            2. Initialize changed = True
            3. While changed:
                a. changed = False
                b. For each filtered entity:
                    - Get all tables that reference this entity (from graph)
                    - For tables already synced, query distinct FK values
                    - Add new IDs to result[entity]
                    - If new IDs found: changed = True
            4. Return result

        Example:
            Pass 1: Extract account IDs from junction tables → 17 accounts
            Pass 2: Extract contact IDs from those 17 accounts → 2,175 contacts
            Pass 3: Extract account IDs from those 2,175 contacts → 0 new (convergence)
        """
        result = {entity: set() for entity in filtered_entities}
        iteration = 0

        while iteration < FilteredSyncManager.MAX_ITERATIONS:
            iteration += 1
            changed = False

            print(f"    Iteration {iteration}:")

            for entity_api_name in filtered_entities:
                # Get all tables/columns that reference this entity
                references = relationship_graph.get_entities_that_reference(entity_api_name)

                for table_name, fk_column in references:
                    # Query distinct FK values from this table
                    fk_values = db_manager.query_distinct_values(table_name, fk_column)

                    # Add new IDs
                    old_count = len(result[entity_api_name])
                    result[entity_api_name].update(fk_values)
                    new_count = len(result[entity_api_name])

                    if new_count > old_count:
                        added = new_count - old_count
                        print(
                            f"      {entity_api_name}: +{added} from {table_name}.{fk_column} "
                            f"(total: {new_count})",
                        )
                        changed = True

            if not changed:
                print(f"    Converged after {iteration} iterations")
                break

        if iteration >= FilteredSyncManager.MAX_ITERATIONS:
            print(f"    ⚠️  Warning: Reached max iterations ({FilteredSyncManager.MAX_ITERATIONS})")

        return result

    def __init__(
        self,
        client: DataverseClient,
        db_manager: DatabaseManager,
        state_manager: SyncStateManager,
    ):
        """
        Initialize filtered sync manager.

        Args:
            client: Dataverse API client
            db_manager: Database manager
            state_manager: Sync state manager
        """
        self.client = client
        self.db_manager = db_manager
        self.state_manager = state_manager

    async def sync_filtered_entity(
        self,
        entity: EntityConfig,
        ids: set[str],
        schema: TableSchema,
    ) -> tuple[int, int]:
        """
        Sync a filtered entity using batched $filter queries.

        Args:
            entity: Entity configuration
            ids: Set of IDs to sync
            schema: Dataverse schema for this entity

        Returns:
            Tuple of (added, updated) counts

        Algorithm:
            1. Get primary key column name from schema
            2. Get last sync timestamp for incremental support
            3. Split IDs into batches of 50
            4. For each batch:
                a. Build filter: "pk eq 'id1' or pk eq 'id2' or ..."
                b. Add modifiedon filter if incremental: "and modifiedon gt {timestamp}"
                c. Fetch with pagination
                d. UPSERT records
            5. Save new max timestamp
            6. Return (added, updated) counts
        """
        if not ids:
            return 0, 0

        # Start sync
        log_id = self.state_manager.start_sync(entity.api_name)

        try:
            # Get primary key from schema with fallback logic
            primary_key = schema.primary_key
            if not primary_key:
                msg = f"No primary key found for {entity.api_name}"
                raise ValueError(msg)

            # Handle case where $metadata primary_key doesn't exist in actual columns
            # (e.g., systemuser has ownerid in metadata but systemuserid is the real PK)
            column_names = [c.name for c in schema.columns]
            if primary_key not in column_names:
                # Try fallback: {entity_name}id
                fallback_pk = f"{entity.name}id"
                if fallback_pk in column_names:
                    print(
                        f"    ⚠️  Primary key '{primary_key}' not in columns, "
                        f"using '{fallback_pk}' instead",
                    )
                    primary_key = fallback_pk
                else:
                    # Find any column ending with 'id' that's not a FK
                    id_cols = [
                        name
                        for name in column_names
                        if name.endswith("id") and not name.startswith("_")
                    ]
                    if id_cols:
                        print(
                            f"    ⚠️  Primary key '{primary_key}' not in columns, "
                            f"using '{id_cols[0]}' instead",
                        )
                        primary_key = id_cols[0]
                    else:
                        msg = f"Cannot find valid primary key for {entity.api_name}"
                        raise ValueError(msg)

            # Get last timestamp for incremental sync
            last_timestamp = self.db_manager.get_last_sync_timestamp(entity.api_name)

            # Initialize counters
            total_added = 0
            total_updated = 0
            all_records = []

            # Split IDs into new vs. already-synced to avoid filtering new IDs by timestamp
            # Check which IDs already exist in the database
            existing_ids = set()
            if last_timestamp:
                # Only check if we have a timestamp (meaning we've synced before)
                cursor = self.db_manager.conn.cursor()
                for id_val in ids:
                    cursor.execute(
                        f"SELECT 1 FROM {entity.api_name} WHERE {primary_key} = ? LIMIT 1",
                        (id_val,),
                    )
                    if cursor.fetchone():
                        existing_ids.add(id_val)

            new_ids = ids - existing_ids

            # Convert to lists for batching
            new_id_list = list(new_ids)
            existing_id_list = list(existing_ids)

            # Sync NEW IDs (without timestamp filter)
            for i in range(0, len(new_id_list), self.BATCH_SIZE):
                batch = new_id_list[i : i + self.BATCH_SIZE]

                # Build $filter query: "pk eq 'id1' or pk eq 'id2' or ..."
                filter_parts = [f"{primary_key} eq '{record_id}'" for record_id in batch]
                filter_query = " or ".join(filter_parts)

                # NO timestamp filter for new IDs!

                # Fetch records with pagination
                records = await self.client.fetch_all_pages(
                    entity.api_name,
                    orderby=primary_key,
                    filter_query=filter_query,
                )

                all_records.extend(records)

            # Sync EXISTING IDs (with timestamp filter for incremental updates)
            if existing_ids and last_timestamp and "modifiedon" in [c.name for c in schema.columns]:
                for i in range(0, len(existing_id_list), self.BATCH_SIZE):
                    batch = existing_id_list[i : i + self.BATCH_SIZE]

                    # Build $filter query with timestamp
                    filter_parts = [f"{primary_key} eq '{record_id}'" for record_id in batch]
                    filter_query = " or ".join(filter_parts)
                    filter_query = f"({filter_query}) and modifiedon gt {last_timestamp}"

                    # Fetch records with pagination
                    records = await self.client.fetch_all_pages(
                        entity.api_name,
                        orderby=primary_key,
                        filter_query=filter_query,
                    )

                    all_records.extend(records)

            # UPSERT all records using batch method (handles @odata fields)
            if all_records:
                total_added, total_updated = self.db_manager.upsert_batch(
                    entity.api_name,
                    primary_key,
                    schema,
                    all_records,
                )

                # Update timestamp
                timestamps = [r["modifiedon"] for r in all_records if r.get("modifiedon")]
                if timestamps:
                    max_timestamp = max(timestamps)
                    self.db_manager.update_sync_timestamp(
                        entity.api_name,
                        max_timestamp,
                        len(all_records),
                    )

            # Complete sync
            self.state_manager.complete_sync(log_id, entity.api_name, total_added, total_updated)

            return total_added, total_updated

        except Exception as e:
            # Fail sync
            self.state_manager.fail_sync(log_id, entity.api_name, str(e))
            raise
