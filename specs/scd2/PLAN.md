# SCD2 Implementation Plan

## Overview

Implement SCD2 (Slowly Changing Dimension Type 2) to track full history of record changes. When records are updated in Dataverse, create new rows instead of overwriting existing ones.

**User Requirements:**
- Use `valid_to IS NULL` to filter active records (no `is_active` column)
- Add surrogate key `row_id` as primary key
- Track changes from first sync forward
- Delete existing database and resync with new schema (no migration needed)

## Current Implementation

**Schema:**
- Business key (e.g., `accountid`) is PRIMARY KEY
- Special columns: `json_response`, `sync_time`, `valid_from`
- `valid_from` populated from Dataverse `modifiedon`

**UPSERT Logic:**
- Uses `INSERT OR REPLACE` to overwrite records
- Checks if record exists, returns `is_new` boolean
- Location: `lib/sync/database.py:272-306`

## Schema Changes

### 1. Table Structure (schema_initializer.py)

**Add surrogate primary key as FIRST column:**
```python
# Line 34
column_defs.append("  row_id INTEGER PRIMARY KEY AUTOINCREMENT")
```

**Remove PRIMARY KEY constraint from business key:**
```python
# Remove lines 40-41 (the PRIMARY KEY logic)
# Business keys become regular indexed columns
```

**Add valid_to column:**
```python
# Line 56-57
if "valid_to" in special_columns:
    column_defs.append("  valid_to TEXT")
```

**Update special_columns parameter:**
```python
# Line 123
special_columns=["json_response", "sync_time", "valid_from", "valid_to"]
```

### 2. Indexes (schema_initializer.py:136-150)

**Add three new indexes:**

1. **Business key index** - For fast lookups by business key
   ```python
   db_manager.create_index(plural_name, schema.primary_key)
   ```

2. **Composite index** - For efficient active record queries
   ```python
   index_name = f"idx_{plural_name}_{schema.primary_key}_valid_to"
   sql = f"CREATE INDEX IF NOT EXISTS {index_name} ON {plural_name}({schema.primary_key}, valid_to)"
   db_manager.execute(sql)
   ```

3. **Valid_to index** - For time-travel queries
   ```python
   db_manager.create_index(plural_name, "valid_to")
   ```

## UPSERT Logic Changes

### New SCD2 Method (database.py:308-407)

**Add new method `upsert_scd2()`:**

```python
def upsert_scd2(self, table_name: str, business_key: str, record: dict) -> bool:
    """Insert or update record using SCD2 logic."""

    # Step 1: Find active record (valid_to IS NULL)
    cursor.execute(
        f"SELECT row_id, json_response FROM {table_name} "
        f"WHERE {business_key} = ? AND valid_to IS NULL",
        (record[business_key],)
    )
    active_record = cursor.fetchone()

    # Step 2: If no active record, INSERT new
    if active_record is None:
        columns = list(record.keys()) + ["valid_to"]
        values = list(record.values()) + [None]
        sql = f"INSERT INTO {table_name} ({','.join(columns)}) VALUES ({placeholders})"
        cursor.execute(sql, values)
        self.conn.commit()
        return True  # New record

    # Step 3: Check if data changed (compare json_response)
    row_id, old_json = active_record
    if old_json == record["json_response"]:
        # No change - update sync_time only
        cursor.execute(
            f"UPDATE {table_name} SET sync_time = ? WHERE row_id = ?",
            (record["sync_time"], row_id)
        )
        self.conn.commit()
        return False

    # Step 4: Data changed - close old record and insert new
    cursor.execute(
        f"UPDATE {table_name} SET valid_to = ? WHERE row_id = ?",
        (record["valid_from"], row_id)
    )

    columns = list(record.keys()) + ["valid_to"]
    values = list(record.values()) + [None]
    sql = f"INSERT INTO {table_name} ({','.join(columns)}) VALUES ({placeholders})"
    cursor.execute(sql, values)
    self.conn.commit()
    return False  # Update
```

**Update `upsert_batch()` to use SCD2:**
```python
# Line 478: Change from
is_new = self.upsert(table_name, primary_key, record)

# To
is_new = self.upsert_scd2(table_name, primary_key, record)
```

## Testing Strategy

### Unit Tests (tests/unit/sync/test_database.py:71-258)

Add test class `TestSCD2Operations` with 5 tests:

1. **test_scd2_insert_new_record** - Verify new record has `valid_to = NULL`
2. **test_scd2_update_closes_old_and_inserts_new** - Verify old record closed, new inserted
3. **test_scd2_no_change_no_new_version** - Verify unchanged data doesn't create version
4. **test_scd2_query_active_records** - Verify `WHERE valid_to IS NULL` returns only active
5. **test_scd2_multiple_records** - Verify multiple records work correctly

### Integration Tests (tests/e2e/test_integration_sync.py)

**Update existing test:** `test_incremental_sync`
- Query active records with `WHERE valid_to IS NULL`
- Verify 2 total records (historical + current)
- Verify 1 active record

## Edge Cases

### Junction Tables
- Do NOT use SCD2 (no `modifiedon` field)
- Continue using existing `clear_junction_records()` + re-insert pattern
- History preserved in main entity's `json_response`

### Option Set Tables
- Do NOT use SCD2
- Continue updating labels in place
- Reference data, not transactional data

### Entities Without `modifiedon`
- Already handled: `valid_from` fallback in line 475
  ```python
  record["valid_from"] = api_record.get("modifiedon")
  ```
- If `modifiedon` is None, `valid_from` will be None
- Every sync creates new version (less ideal but functional)

### Filtered Entities
- Already use `upsert_batch()` - will automatically use SCD2
- No changes needed to `filtered_sync.py`

## Implementation Sequence

### Phase 1: Schema Changes
1. Update `generate_create_table_sql()` - add row_id, remove PK constraint, add valid_to
2. Update `initialize_tables()` - include valid_to in special columns
3. Add SCD2 indexes (business key, composite, valid_to)

### Phase 2: SCD2 UPSERT Logic
1. Implement `upsert_scd2()` method
2. Update `upsert_batch()` to use `upsert_scd2()`

### Phase 3: Testing
1. Add unit tests for SCD2 operations
2. Update integration test for SCD2 queries
3. Verify all 102 tests pass

### Phase 4: Production Deployment
1. Delete existing database: `rm dataverse_complete.db`
2. Run sync: `python sync_dataverse.py`
3. Tables created with SCD2 schema automatically
4. Option sets loaded from `config/optionsets.json`

## Querying Active Records

**Active records only:**
```sql
SELECT * FROM accounts WHERE valid_to IS NULL
```

**All versions of a record:**
```sql
SELECT row_id, accountid, name, valid_from, valid_to
FROM accounts
WHERE accountid = '...'
ORDER BY valid_from
```

**Point-in-time query:**
```sql
SELECT * FROM accounts
WHERE accountid = '...'
  AND valid_from <= '2024-02-15T00:00:00Z'
  AND (valid_to IS NULL OR valid_to > '2024-02-15T00:00:00Z')
```

## Critical Files Modified

1. **lib/sync/schema_initializer.py** (lines 28-57, 123, 136-150)
   - Schema generation with row_id, valid_to, indexes

2. **lib/sync/database.py** (lines 308-407, 478)
   - SCD2 upsert logic

3. **tests/unit/sync/test_database.py** (lines 71-258)
   - Unit tests for SCD2 operations

4. **tests/e2e/test_integration_sync.py** (lines 214-249)
   - Updated integration test for SCD2

## Success Criteria

✅ All historical versions preserved with correct `valid_from`/`valid_to`
✅ Active record queries return only current versions (`valid_to IS NULL`)
✅ No duplicate active records
✅ Junction tables and option sets continue to function
✅ All 102 tests pass
✅ Simple deployment: delete DB and resync (no migration complexity)

## Answer to Key Question

**Q: Is `valid_from` and `valid_to` sufficient for filtering active records, or do we need an `active` column?**

**A:** `valid_from` and `valid_to` are sufficient. The pattern `WHERE valid_to IS NULL` efficiently identifies active records. With the composite index on `(business_key, valid_to)`, these queries are fast and follow standard SCD2 conventions. No `active` column needed.
