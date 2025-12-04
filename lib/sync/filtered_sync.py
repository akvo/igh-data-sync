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

    def _resolve_primary_key(self, schema: TableSchema, entity: EntityConfig) -> str:
        """
        Resolve primary key column name with fallback logic.

        Tries in order:
        1. schema.primary_key if it exists in columns
        2. {entity.name}id if it exists
        3. First column ending with 'id' that's not a FK

        Args:
            schema: Table schema
            entity: Entity configuration

        Returns:
            Primary key column name

        Raises:
            ValueError: If no valid primary key found
        """
        primary_key = schema.primary_key
        if not primary_key:
            msg = f"No primary key found for {entity.api_name}"
            raise ValueError(msg)

        column_names = [c.name for c in schema.columns]

        # Check if primary key exists in columns
        if primary_key in column_names:
            return primary_key

        # Fallback 1: {entity_name}id
        fallback_pk = f"{entity.name}id"
        if fallback_pk in column_names:
            print(
                f"    ⚠️  Primary key '{primary_key}' not in columns, "
                f"using '{fallback_pk}' instead",
            )
            return fallback_pk

        # Fallback 2: Any column ending with 'id' that's not a FK
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
            return id_cols[0]

        msg = f"Cannot find valid primary key for {entity.api_name}"
        raise ValueError(msg)

    def _separate_new_and_existing_ids(
        self,
        ids: set[str],
        entity_api_name: str,
        primary_key: str,
        last_timestamp: str | None,
    ) -> tuple[set[str], set[str]]:
        """
        Separate IDs into new (never synced) vs existing (check for updates).

        Args:
            ids: All IDs to sync
            entity_api_name: Entity API name
            primary_key: Primary key column name
            last_timestamp: Last sync timestamp (None if first sync)

        Returns:
            Tuple of (new_ids, existing_ids)
        """
        if not last_timestamp:
            return ids, set()

        existing_ids = set()
        cursor = self.db_manager.conn.cursor()
        for id_val in ids:
            # S608: SQL safe - table/column names from EntityConfig/TableSchema (not user input), values parameterized
            cursor.execute(
                f"SELECT 1 FROM {entity_api_name} WHERE {primary_key} = ? LIMIT 1",  # noqa: S608
                (id_val,),
            )
            if cursor.fetchone():
                existing_ids.add(id_val)

        new_ids = ids - existing_ids
        return new_ids, existing_ids

    async def _fetch_id_batch(
        self,
        batch: list[str],
        primary_key: str,
        entity_api_name: str,
        timestamp_filter: str | None = None,
    ) -> list[dict]:
        """
        Fetch records for a batch of IDs with optional timestamp filter.

        Args:
            batch: List of IDs to fetch
            primary_key: Primary key column name
            entity_api_name: Entity API name
            timestamp_filter: Optional "modifiedon gt {timestamp}" filter

        Returns:
            List of API records
        """
        # Build $filter query: "pk eq 'id1' or pk eq 'id2' or ..."
        filter_parts = [f"{primary_key} eq '{record_id}'" for record_id in batch]
        filter_query = " or ".join(filter_parts)

        # Add timestamp filter if provided
        if timestamp_filter:
            filter_query = f"({filter_query}) and {timestamp_filter}"

        # Fetch records with pagination
        return await self.client.fetch_all_pages(
            entity_api_name,
            orderby=primary_key,
            filter_query=filter_query,
        )

    def _update_sync_timestamp_from_records(
        self,
        entity_api_name: str,
        records: list[dict],
    ) -> None:
        """
        Update sync timestamp from modifiedon values in records.

        Args:
            entity_api_name: Entity API name
            records: List of records with modifiedon field
        """
        timestamps = [r["modifiedon"] for r in records if r.get("modifiedon")]
        if timestamps:
            max_timestamp = max(timestamps)
            self.db_manager.update_sync_timestamp(
                entity_api_name,
                max_timestamp,
                len(records),
            )

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
            # Resolve primary key with fallback logic
            primary_key = self._resolve_primary_key(schema, entity)

            # Get last timestamp for incremental sync
            last_timestamp = self.db_manager.get_last_sync_timestamp(entity.api_name)

            # Separate new vs existing IDs
            new_ids, existing_ids = self._separate_new_and_existing_ids(
                ids, entity.api_name, primary_key, last_timestamp
            )

            all_records = []

            # Sync new IDs (without timestamp filter)
            for i in range(0, len(new_ids), self.BATCH_SIZE):
                batch = list(new_ids)[i : i + self.BATCH_SIZE]
                records = await self._fetch_id_batch(batch, primary_key, entity.api_name)
                all_records.extend(records)

            # Sync existing IDs (with timestamp filter for incremental updates)
            if existing_ids and last_timestamp and "modifiedon" in [c.name for c in schema.columns]:
                timestamp_filter = f"modifiedon gt {last_timestamp}"
                for i in range(0, len(existing_ids), self.BATCH_SIZE):
                    batch = list(existing_ids)[i : i + self.BATCH_SIZE]
                    records = await self._fetch_id_batch(
                        batch, primary_key, entity.api_name, timestamp_filter
                    )
                    all_records.extend(records)

            # UPSERT all records
            total_added = total_updated = 0
            if all_records:
                total_added, total_updated = self.db_manager.upsert_batch(
                    entity.api_name, primary_key, schema, all_records
                )
                self._update_sync_timestamp_from_records(entity.api_name, all_records)

            self.state_manager.complete_sync(log_id, entity.api_name, total_added, total_updated)
            return total_added, total_updated

        except Exception as e:
            self.state_manager.fail_sync(log_id, entity.api_name, str(e))
            raise
