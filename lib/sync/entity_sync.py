"""
Entity synchronization functions.

Handles syncing individual entities from Dataverse to SQLite.
"""


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
        if last_timestamp and "modifiedon" in [c.name for c in schema.columns]:
            filter_query = f"modifiedon gt {last_timestamp}"

        # Determine orderby
        orderby = None
        if schema.primary_key:
            orderby = schema.primary_key
        elif "createdon" in [c.name for c in schema.columns]:
            orderby = "createdon"
        elif "modifiedon" in [c.name for c in schema.columns]:
            orderby = "modifiedon"

        # Fetch all pages
        records = await client.fetch_all_pages(
            entity.api_name,
            orderby=orderby,
            filter_query=filter_query,
        )

        if not records:
            state_manager.complete_sync(log_id, entity.api_name, 0, 0)
            print(f"  ✓ {entity.api_name}: No records")
            return 0, 0

        # Determine actual primary key and upsert
        column_names = [c.name for c in schema.columns]
        actual_pk = _determine_actual_primary_key(schema, entity, records, column_names)
        added, updated = db_manager.upsert_batch(entity.api_name, actual_pk, schema, records)

        # Update timestamp
        _update_sync_timestamp(db_manager, entity.api_name, records)

        state_manager.complete_sync(log_id, entity.api_name, added, updated)
        print(f"  ✓ {entity.api_name}: {added} added, {updated} updated")

        return added, updated  # noqa: TRY300 - clear flow, no benefit from else block

    except Exception as e:
        state_manager.fail_sync(log_id, entity.api_name, str(e))
        print(f"  ❌ {entity.api_name}: Failed - {e}")
        raise


def _determine_actual_primary_key(schema, entity, records, column_names):
    """
    Determine the actual primary key to use for UPSERT.

    Handles cases where $metadata primary_key doesn't exist in actual columns.
    """
    actual_pk = schema.primary_key

    if actual_pk and actual_pk not in column_names:
        # Primary key from metadata doesn't exist in columns (e.g., systemuser's ownerid)
        # Try to find alternative: {entity_name}id
        fallback_pk = f"{entity.name}id"
        if fallback_pk in column_names:
            actual_pk = fallback_pk
            print(
                f"    ⚠️  Primary key '{schema.primary_key}' not in columns, using '{actual_pk}' instead",
            )
        elif fallback_pk in records[0]:
            # It's in the API response but not in schema columns - add it
            actual_pk = fallback_pk
            print(
                f"    ⚠️  Primary key '{schema.primary_key}' not in columns, using '{actual_pk}' from API response",
            )
        else:
            # Last resort: find any column ending with 'id' that exists in both schema and data
            id_cols = [name for name in column_names if name.endswith("id") and not name.startswith("_")]
            if id_cols:
                actual_pk = id_cols[0]
                print(
                    f"    ⚠️  Primary key '{schema.primary_key}' not in columns, using '{actual_pk}' instead",
                )
            else:
                msg = f"Cannot find valid primary key for {entity.api_name}"
                raise RuntimeError(msg)

    return actual_pk


def _update_sync_timestamp(db_manager, entity_api_name, records):
    """Update the sync timestamp based on records."""
    if records:
        # Get all non-null modifiedon values
        timestamps = [r["modifiedon"] for r in records if r.get("modifiedon")]
        print(
            f"    DEBUG: Found {len(timestamps)} records with modifiedon out of {len(records)} total",
        )
        if timestamps:
            max_timestamp = max(timestamps)
            print(f"    DEBUG: Saving timestamp {max_timestamp}")
            db_manager.update_sync_timestamp(entity_api_name, max_timestamp, len(records))
        else:
            print("    DEBUG: No timestamps found, not saving")
