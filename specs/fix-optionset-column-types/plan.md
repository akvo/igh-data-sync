# Implementation Plan: Fix Option Set Column Types (TEXT → INTEGER)

## Status Note
✅ **Previous Issue Resolved**: The vin_candidates sync failure documented in the original version of this plan has been fixed (60-second timeout override removed from `lib/dataverse_client.py:184`).

This plan now addresses a new issue discovered after the option set implementation.

---

## Problem Statement

Option set fields (e.g., `new_globalhealtharea` in `vin_diseases`) are currently created as **TEXT** columns instead of **INTEGER** columns, even though they store integer codes and should have proper foreign key relationships to option set lookup tables.

### Current Behavior (Incorrect)
```sql
-- Entity table has TEXT column
CREATE TABLE vin_diseases (
  vin_diseaseid TEXT PRIMARY KEY,
  new_globalhealtharea TEXT,  -- ❌ Should be INTEGER
  ...
);

-- Lookup table exists with INTEGER code
CREATE TABLE _optionset_new_globalhealtharea (
  code INTEGER PRIMARY KEY,
  label TEXT NOT NULL,
  first_seen TEXT NOT NULL
);

-- Joins work (SQLite implicit conversion) but semantically wrong
SELECT d.vin_diseaseid, o.label
FROM vin_diseases d
LEFT JOIN _optionset_new_globalhealtharea o
  ON d.new_globalhealtharea = o.code;  -- TEXT = INTEGER (works but wrong)
```

### Desired Behavior (Correct)
```sql
CREATE TABLE vin_diseases (
  vin_diseaseid TEXT PRIMARY KEY,
  new_globalhealtharea INTEGER,  -- ✅ Correct type
  FOREIGN KEY (new_globalhealtharea)
    REFERENCES _optionset_new_globalhealtharea(code)
);
```

---

## Root Cause Analysis

### Why Are Option Sets Created as TEXT?

**Timeline of schema creation:**

1. **Step [1/7]**: Fetch $metadata XML from Dataverse
2. **Step [2/7]**: Parse entity schemas from $metadata
3. **Step [3/7]**: Map EDM types to SQLite types
4. **Step [4/7]**: **CREATE TABLE statements executed** ← Tables created here
5. **Step [5/7]**: Sync data from Dataverse API
6. **Step [6/7]**: **Detect option sets from FormattedValue annotations** ← Too late!
7. **Step [7/7]**: Populate option set lookup/junction tables

**The Problem**: Option sets are only detected AFTER tables are created.

### Technical Details

**File: `lib/validation/metadata_parser.py`**
```python
def _parse_properties(self, entity_elem: ET.Element, ns: dict[str, str]):
    for prop_elem in entity_elem.findall("edm:Property", ns):
        edm_type = prop_elem.get("Type")  # e.g., "Edm.String" for option sets
        db_type = map_edm_to_db_type(edm_type, ...)  # Maps "Edm.String" → "TEXT"
```

**File: `lib/type_mapping.py`**
```python
EDM_TYPE_MAP_SQLITE = {
    "Edm.String": "TEXT",  # ← Option sets appear as Edm.String in metadata
    "Edm.Int16": "INTEGER",
    "Edm.Int32": "INTEGER",
    # ...
}
```

**Why Edm.String?** Dataverse serializes option set values as strings in the $metadata XML, even though the actual API returns integers. The metadata has no way to distinguish an option set field from a regular text field.

---

## Evaluated Approaches

### Approach 1: Configuration-Driven Type Override ❌
**Description**: Manually list option set fields in `entities_config.json`

**Pros**:
- Clean separation of concerns
- Explicit control over which fields are option sets

**Cons**:
- ❌ Requires manual identification of 70+ option set fields across 26 entities
- ❌ Maintenance burden (must update config when Dataverse schema changes)
- ❌ Violates "infer from data" philosophy (Plan C)

**Verdict**: Rejected - too much manual work, breaks automation

---

### Approach 2: ALTER TABLE Post-Detection ❌
**Description**: Create tables as TEXT, then ALTER TABLE after detecting option sets

**Technical Challenge**: SQLite doesn't support `ALTER COLUMN` directly. Must:
1. Rename old table
2. Create new table with correct types
3. Copy data
4. Drop old table

**Pros**:
- No metadata changes needed
- Works with existing detection logic

**Cons**:
- ❌ Complex migration logic (risky)
- ❌ No ALTER TABLE capability exists currently
- ❌ Potential data loss if migration fails mid-process
- ❌ Must handle foreign key constraints carefully

**Verdict**: Rejected - too complex, too risky

---

### Approach 3: Deferred Schema Creation ❌
**Description**: Sync some data first, THEN create tables with correct types

**Pros**:
- Option sets detected before schema creation
- No ALTER TABLE needed

**Cons**:
- ❌ Major architectural change (schema creation is step [4/7])
- ❌ Where to store data before tables exist?
- ❌ Breaks existing flow expectations
- ❌ High risk of breaking other parts of the system

**Verdict**: Rejected - too invasive

---

### Approach 4: Sample Data Pre-Analysis ✅ **RECOMMENDED**
**Description**: Fetch 1-10 sample records per entity BEFORE schema creation, detect option sets from samples, override type mapping

**Flow**:
1. Fetch $metadata XML (existing step)
2. **NEW**: For each entity, fetch sample records (e.g., `?$top=10`)
3. **NEW**: Detect option sets from samples using existing `OptionSetDetector`
4. **NEW**: Pass detected option set fields to schema parser
5. Create tables with INTEGER for detected option sets, TEXT for others
6. Proceed with normal sync (detect option sets again for completeness)

**Implementation Points**:
- **File**: `lib/sync/schema_initializer.py` - Add sample data fetching before table creation
- **File**: `lib/validation/metadata_parser.py` - Accept optional `option_set_fields` parameter to override type mapping
- **File**: `lib/type_mapping.py` - Add logic to check if field is in option_set_fields, return INTEGER if yes

**Pros**:
- ✅ Maintains "infer from data" philosophy (Plan C)
- ✅ Minimal code changes (add one step, modify type mapper)
- ✅ Automatic detection (no manual config)
- ✅ Works for all entities without special-casing
- ✅ Low risk (doesn't change existing data sync flow)
- ✅ API overhead is minimal (~26 extra requests, ~10 records each = ~260 records total, <1% overhead)

**Cons**:
- ⚠️ Requires one extra API call per entity (~26 entities = 26 calls)
- ❌ **CRITICAL FLAW**: Assumes sample data is representative - if first 10-50 records have NULL for option set fields, they won't be detected (FormattedValue only present when value is non-NULL)
- ❌ **User reports**: "Many of these records have no value for their option set columns" - makes this approach unreliable

**Verdict**: ❌ **REJECTED** - Unreliable for sparse data (common in Dataverse)

---

### Approach 5: Metadata Injection Hack ❌
**Description**: Modify the $metadata XML after fetching to mark option sets as `Edm.Int32`

**Pros**:
- Minimal changes to type mapper

**Cons**:
- ❌ Requires fetching sample data anyway (to know which fields to modify)
- ❌ Same sparse data problem as Approach 4
- ❌ Fragile XML manipulation
- ❌ Confusing for future maintainers

**Verdict**: Rejected - overly clever, no real benefit

---

### Approach 6: Configuration-Driven with Auto-Generation ✅
**Description**: Run full sync once with TEXT columns, then generate config file from detected option sets, use config for future syncs

**Two-Phase Process**:

**Phase 1: Initial Discovery (run once)**
```bash
# Sync with TEXT columns (current behavior)
python sync_dataverse.py

# Generate option set config from synced data
python generate_optionset_config.py > config/optionsets.json
```

**Phase 2: Use Config (all future syncs)**
```bash
# Sync with INTEGER columns (uses config/optionsets.json)
rm dataverse_complete.db
python sync_dataverse.py  # Reads config, creates INTEGER columns
```

**Generated Config Example**:
```json
{
  "vin_disease": ["statuscode", "new_globalhealtharea"],
  "vin_candidate": ["statuscode", "vin_whoprequalification"],
  "vin_product": ["statuscode"]
}
```

**Implementation**:
- **New script**: `generate_optionset_config.py` - Queries existing database, finds all `_optionset_*` tables, outputs JSON
- **Modify**: `lib/sync/schema_initializer.py` - Load config if exists, pass to schema fetcher
- **Modify**: Type mapping pipeline (same as Approach 4)

**Pros**:
- ✅ **Reliable**: Detects option sets from ALL data, not just samples
- ✅ **Works with sparse data**: No problem if many records have NULLs
- ✅ **One-time discovery**: Only need to sync once to generate config
- ✅ **Committed to repo**: Config becomes part of project, survives DB recreates
- ✅ **Manual override possible**: Users can edit config if needed
- ✅ **Low ongoing overhead**: Config read once at startup, no extra API calls

**Cons**:
- ⚠️ **Manual step**: User must run generator script after first sync
- ⚠️ **Requires initial sync**: First sync still has TEXT columns (can recreate DB after)
- ⚠️ **Config maintenance**: If Dataverse schema changes (new option sets added), must regenerate config

**Verdict**: ✅ **VIABLE** - Reliable for sparse data, simple to implement

---

### Approach 7: ALTER TABLE After Full Sync (Automatic) ✅
**Description**: Sync with TEXT columns, detect option sets during sync (as now), automatically ALTER tables to INTEGER at end of sync

**Flow**:
1. Create tables with TEXT for all Edm.String fields (as now)
2. Sync data and detect option sets (as now)
3. **NEW**: At end of sync, for each detected option set field:
   - Check if column is TEXT
   - If yes, ALTER table to change column to INTEGER

**SQLite ALTER TABLE Pattern** (CREATE-COPY-DROP):
```python
def alter_column_to_integer(table_name: str, column_name: str):
    # 1. Get current table schema
    cursor.execute(f"SELECT sql FROM sqlite_master WHERE name='{table_name}'")
    create_sql = cursor.fetchone()[0]

    # 2. Modify schema string to change column type
    new_create_sql = create_sql.replace(
        f"{column_name} TEXT",
        f"{column_name} INTEGER"
    )

    # 3. Rename old table
    cursor.execute(f"ALTER TABLE {table_name} RENAME TO {table_name}_old")

    # 4. Create new table with correct types
    cursor.execute(new_create_sql.replace(f"CREATE TABLE {table_name}",
                                         f"CREATE TABLE {table_name}"))

    # 5. Copy data
    cursor.execute(f"INSERT INTO {table_name} SELECT * FROM {table_name}_old")

    # 6. Drop old table
    cursor.execute(f"DROP TABLE {table_name}_old")

    # 7. Recreate indexes
    # (copy index definitions from sqlite_master)
```

**Implementation**:
- **New file**: `lib/sync/schema_migration.py` - ALTER TABLE logic
- **Modify**: `lib/sync/entity_sync.py` or `sync_dataverse.py` - Call migration after sync completes
- **Track**: Which tables/columns need migration (during sync, collect detected option sets)

**Pros**:
- ✅ **Fully automatic**: No manual steps, no config files
- ✅ **Works with sparse data**: Uses ALL synced data for detection
- ✅ **One-time migration**: After first ALTER, subsequent syncs keep INTEGER
- ✅ **Self-healing**: If new option sets detected, automatically migrates
- ✅ **No initial setup**: Just run sync, migration happens automatically

**Cons**:
- ⚠️ **Complex logic**: CREATE-COPY-DROP pattern is error-prone
- ⚠️ **Migration risk**: Data loss if migration fails mid-process
- ⚠️ **Transaction handling**: Must wrap in transaction, handle rollback
- ⚠️ **Foreign keys**: Must disable and re-enable foreign key constraints during migration
- ⚠️ **Indexes**: Must recreate all indexes after migration
- ⚠️ **Performance**: Migration adds time to first sync (but only first sync)

**Mitigation**:
- Wrap entire migration in transaction
- Add detailed logging
- Test extensively with SQLite's integrity checks (`PRAGMA integrity_check`)
- Add dry-run mode for testing

**Verdict**: ✅ **VIABLE** - More complex but fully automatic

---

## Recommended Implementation: Approach 6 (Configuration with Auto-Generation)

### Why Approach 6?

After user feedback revealing that "many records have no value for their option set columns", Approach 4 (sample data pre-analysis) is **unreliable** - NULL values don't have FormattedValue annotations, so sparse data won't be detected.

**Approach 6** solves this by:
1. Running a full sync ONCE with TEXT columns (as currently happens)
2. Detecting ALL option sets from ALL synced data (already happens)
3. Generating a config file from detected option sets (NEW script)
4. Using that config for all future syncs (reads config, creates INTEGER columns)

This is **reliable** (uses ALL data), **simple** (config file + type mapping changes), and **maintainable** (config committed to repo).

### High-Level Changes

**1. Create config generator script**
   - Query existing database to find all `_optionset_*` tables
   - Map option set tables back to entity fields
   - Output JSON config file

**2. Modify schema initialization to load config**
   - Check if `config/optionsets.json` exists
   - Load config and pass to type mapper
   - If not exists, proceed with TEXT columns (as now)

**3. Modify type mapping to check config**
   - In `map_edm_to_db_type()`, if field is in config AND Edm.String, return INTEGER
   - Otherwise use existing logic

**4. Keep existing sync-time detection**
   - Continue detecting option sets during sync (as now)
   - Create lookup/junction tables as before

### Detailed Implementation

#### File 1: `generate_optionset_config.py` (NEW)

**Purpose**: Analyze existing database to generate option set configuration

**Location**: Root directory (alongside `sync_dataverse.py`)

**Full Implementation**:
```python
#!/usr/bin/env python3
"""Generate option set configuration from synced database.

Usage:
    python generate_optionset_config.py > config/optionsets.json

This script analyzes the SQLite database to find all option set lookup tables
(_optionset_*) and maps them back to entity fields, generating a configuration
file for future syncs.
"""

import json
import sqlite3
import sys
from pathlib import Path


def extract_option_sets(db_path: str) -> dict[str, list[str]]:
    """
    Extract option set fields from database.

    Args:
        db_path: Path to SQLite database

    Returns:
        Dict mapping entity name to list of option set field names
    """
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()

    # Find all option set lookup tables
    cursor.execute("""
        SELECT name FROM sqlite_master
        WHERE type='table' AND name LIKE '_optionset_%'
        ORDER BY name
    """)

    optionset_tables = [row[0] for row in cursor.fetchall()]

    if not optionset_tables:
        print("⚠️  No option set tables found in database", file=sys.stderr)
        return {}

    print(f"Found {len(optionset_tables)} option set tables", file=sys.stderr)

    # Map option set fields to entities
    option_sets_by_entity = {}

    for table in optionset_tables:
        # Extract field name from table name (_optionset_<field_name>)
        field_name = table.replace("_optionset_", "")

        # Find which entity tables have this field
        cursor.execute("""
            SELECT name FROM sqlite_master
            WHERE type='table'
              AND name NOT LIKE '_%'  -- Exclude internal tables
              AND name NOT LIKE 'sqlite_%'  -- Exclude SQLite tables
        """)

        entity_tables = [row[0] for row in cursor.fetchall()]

        for entity_table in entity_tables:
            # Check if this entity has the field
            cursor.execute(f"PRAGMA table_info({entity_table})")
            columns = [col[1] for col in cursor.fetchall()]

            if field_name in columns:
                # Convert table name to singular entity name (simple heuristic)
                # e.g., "vin_diseases" -> "vin_disease"
                if entity_table.endswith("ies"):
                    entity_name = entity_table[:-3] + "y"
                elif entity_table.endswith("ses"):
                    entity_name = entity_table[:-2]
                elif entity_table.endswith("s"):
                    entity_name = entity_table[:-1]
                else:
                    entity_name = entity_table

                if entity_name not in option_sets_by_entity:
                    option_sets_by_entity[entity_name] = []

                option_sets_by_entity[entity_name].append(field_name)
                print(f"  ✓ {entity_name}.{field_name}", file=sys.stderr)

    conn.close()

    # Sort fields for consistency
    for entity in option_sets_by_entity:
        option_sets_by_entity[entity].sort()

    return option_sets_by_entity


def main():
    db_path = "dataverse_complete.db"

    if not Path(db_path).exists():
        print(f"❌ Database not found: {db_path}", file=sys.stderr)
        print(f"   Run sync_dataverse.py first to create the database", file=sys.stderr)
        sys.exit(1)

    print("Analyzing database...", file=sys.stderr)
    option_sets = extract_option_sets(db_path)

    print(f"\n✓ Generated config for {len(option_sets)} entities", file=sys.stderr)
    print(f"  Total option set fields: {sum(len(fields) for fields in option_sets.values())}", file=sys.stderr)
    print("\nSave output to config/optionsets.json, then re-sync from scratch:", file=sys.stderr)
    print("  mkdir -p config", file=sys.stderr)
    print("  python generate_optionset_config.py > config/optionsets.json", file=sys.stderr)
    print("  rm dataverse_complete.db", file=sys.stderr)
    print("  python sync_dataverse.py", file=sys.stderr)
    print("", file=sys.stderr)

    # Output JSON to stdout
    print(json.dumps(option_sets, indent=2))


if __name__ == "__main__":
    main()
```

**Example Output** (`config/optionsets.json`):
```json
{
  "vin_disease": [
    "new_globalhealtharea",
    "statuscode"
  ],
  "vin_candidate": [
    "statuscode",
    "vin_syndromicprofiles",
    "vin_whoprequalification"
  ],
  "vin_product": [
    "statuscode"
  ]
}
```

---

#### File 2: `lib/sync/schema_initializer.py` (MODIFIED)

**Location**: Lines 58-80 (inside `initialize_tables` function)

**Current code**:
```python
async def initialize_tables(_config, entities: list[EntityConfig], client, db_manager):
    # Fetch schemas from $metadata
    fetcher = DataverseSchemaFetcher(client, target_db="sqlite")
    singular_names = [e.name for e in entities]

    print(f"Fetching schemas for {len(entities)} entities from $metadata...")
    schemas = await fetcher.fetch_schemas_from_metadata(singular_names)

    # Create tables
    for entity in entities:
        # ... create table logic
```

**New code**:
```python
async def initialize_tables(_config, entities: list[EntityConfig], client, db_manager):
    import json
    from pathlib import Path

    # STEP 1: Load option set config if exists
    config_path = Path("config/optionsets.json")
    option_set_fields_by_entity = {}

    if config_path.exists():
        print(f"Loading option set configuration from {config_path}...")
        with open(config_path) as f:
            option_set_fields_by_entity = json.load(f)
        total_fields = sum(len(fields) for fields in option_set_fields_by_entity.values())
        print(f"  ✓ Loaded config for {len(option_set_fields_by_entity)} entities, {total_fields} option set fields")
    else:
        print("⚠️  No option set config found - tables will use TEXT for option sets")
        print(f"   To fix: Run sync, then: python generate_optionset_config.py > {config_path}")

    # STEP 2: Fetch schemas from $metadata
    fetcher = DataverseSchemaFetcher(client, target_db="sqlite")
    singular_names = [e.name for e in entities]

    print(f"Fetching schemas for {len(entities)} entities from $metadata...")
    schemas = await fetcher.fetch_schemas_from_metadata(
        singular_names,
        option_set_fields_by_entity=option_set_fields_by_entity if option_set_fields_by_entity else None
    )

    # STEP 3: Create tables (existing logic unchanged)
    for entity in entities:
        # ... create table logic
```

---

#### File 3: `lib/validation/dataverse_schema.py` (MODIFIED)

**Location**: Lines 30-60 (inside `DataverseSchemaFetcher.fetch_schemas_from_metadata`)

**Changes**: Same as Approach 4 - accept and forward `option_set_fields_by_entity` parameter

**Current signature**:
```python
async def fetch_schemas_from_metadata(self, entity_names: list[str]) -> dict[str, TableSchema]:
```

**New signature**:
```python
async def fetch_schemas_from_metadata(
    self,
    entity_names: list[str],
    option_set_fields_by_entity: Optional[dict[str, list[str]]] = None  # Config from JSON
) -> dict[str, TableSchema]:
```

**Implementation**:
```python
async def fetch_schemas_from_metadata(
    self,
    entity_names: list[str],
    option_set_fields_by_entity: Optional[dict[str, list[str]]] = None
) -> dict[str, TableSchema]:

    # Fetch $metadata (unchanged)
    xml_content = await self.client.get_metadata()

    # Parse with option set field info (from config)
    parser = MetadataParser(target_db=self.target_db)
    all_schemas = parser.parse_metadata_xml(
        xml_content,
        option_set_fields_by_entity=option_set_fields_by_entity
    )

    # Filter and return (unchanged)
    return {name: all_schemas[name] for name in entity_names if name in all_schemas}
```

---

#### File 4: `lib/validation/metadata_parser.py` (MODIFIED)

**Changes**: Same logic as Approach 4 - accept option_set_fields and pass through to type mapper

**Note**: `option_set_fields` now comes from config file (list) instead of sample data (set), so convert to set when needed

**Three methods to modify**:

1. **`parse_metadata_xml()` - Accept config and extract per-entity fields**
```python
def parse_metadata_xml(
    self,
    xml_content: str,
    option_set_fields_by_entity: Optional[dict[str, list[str]]] = None  # From config
) -> dict[str, TableSchema]:

    # ... XML parsing setup (unchanged)

    for entity_elem in schema_elem.findall("edm:EntityType", ns):
        entity_name = entity_elem.get("Name")

        # Get option set fields for this entity (convert list to set)
        option_set_fields = set(option_set_fields_by_entity.get(entity_name, [])) if option_set_fields_by_entity else set()

        # Parse entity with option set field info
        table_schema = self._parse_entity_type(
            entity_elem,
            ns,
            option_set_fields=option_set_fields
        )

        schemas[entity_name] = table_schema

    return schemas
```

2. **`_parse_entity_type()` - Pass fields to property parser**
```python
def _parse_entity_type(
    self,
    entity_elem: ET.Element,
    ns: dict[str, str],
    option_set_fields: Optional[set[str]] = None
) -> TableSchema:

    entity_name = entity_elem.get("Name")
    primary_key = self._parse_primary_key(entity_elem, ns)

    # Pass option_set_fields to property parser
    columns = self._parse_properties(
        entity_elem,
        ns,
        option_set_fields=option_set_fields
    )

    foreign_keys = self._parse_all_foreign_keys(entity_elem, ns, columns, primary_key)

    return TableSchema(
        entity_name=entity_name,
        columns=columns,
        primary_key=primary_key,
        foreign_keys=foreign_keys,
    )
```

3. **`_parse_properties()` - Check if field is option set, pass to type mapper**
```python
def _parse_properties(
    self,
    entity_elem: ET.Element,
    ns: dict[str, str],
    option_set_fields: Optional[set[str]] = None
) -> list[ColumnMetadata]:

    if option_set_fields is None:
        option_set_fields = set()

    columns = []

    for prop_elem in entity_elem.findall("edm:Property", ns):
        name = prop_elem.get("Name")
        edm_type = prop_elem.get("Type")
        nullable = prop_elem.get("Nullable", "true").lower() == "true"
        max_length_str = prop_elem.get("MaxLength")
        max_length = int(max_length_str) if max_length_str and max_length_str.isdigit() else None

        # Check if this field is in the option set config
        is_option_set = name in option_set_fields

        # Map EDM type to database type (with option set override)
        db_type = map_edm_to_db_type(
            edm_type,
            self.target_db,
            max_length,
            is_option_set=is_option_set
        )

        column = ColumnMetadata(
            name=name,
            db_type=db_type,
            nullable=nullable,
            max_length=max_length,
        )
        columns.append(column)

    return columns
```

---

#### File 5: `lib/type_mapping.py` (MODIFIED)

**Location**: Lines 70-100 (`map_edm_to_db_type` function)

**Current signature**:
```python
def map_edm_to_db_type(
    edm_type: str,
    target_db: str,
    max_length: Optional[int] = None
) -> str:
```

**New signature**:
```python
def map_edm_to_db_type(
    edm_type: str,
    target_db: str,
    max_length: Optional[int] = None,
    is_option_set: bool = False  # NEW
) -> str:
```

**Changes**:
```python
def map_edm_to_db_type(
    edm_type: str,
    target_db: str,
    max_length: Optional[int] = None,
    is_option_set: bool = False
) -> str:
    """
    Map OData EDM type to database type.

    Args:
        edm_type: EDM type from $metadata (e.g., "Edm.String", "Edm.Int32")
        target_db: Target database ("sqlite" or "postgresql")
        max_length: Optional max length for string types
        is_option_set: If True and edm_type is Edm.String, return INTEGER

    Returns:
        Database type string (e.g., "TEXT", "INTEGER")
    """
    # CRITICAL: Override for option sets
    # Option sets appear as Edm.String in metadata but store integer codes
    if is_option_set and edm_type == "Edm.String":
        return "INTEGER"

    # Normal type mapping (existing logic)
    type_map = EDM_TYPE_MAP_SQLITE if target_db == "sqlite" else EDM_TYPE_MAP_POSTGRES
    base_type = type_map.get(edm_type, "TEXT")

    # Handle VARCHAR with max_length (existing logic)
    if base_type in ("TEXT", "VARCHAR") and max_length and max_length <= 4000:
        if target_db == "postgresql":
            return f"VARCHAR({max_length})"

    return base_type
```

---

### Testing Strategy

**1. Unit Tests**

**File**: `tests/unit/type_mapping/test_optionset_type_override.py` (NEW)
```python
def test_option_set_overrides_edm_string():
    """Option sets should map Edm.String → INTEGER"""
    result = map_edm_to_db_type("Edm.String", "sqlite", is_option_set=True)
    assert result == "INTEGER"

def test_regular_string_not_affected():
    """Regular strings should still map to TEXT"""
    result = map_edm_to_db_type("Edm.String", "sqlite", is_option_set=False)
    assert result == "TEXT"
```

**File**: `tests/unit/validation/test_metadata_parser_optionsets.py` (NEW)
```python
def test_parser_uses_option_set_fields():
    """Parser should pass option_set_fields to type mapper"""
    xml = """
    <EntityType Name="vin_disease">
      <Property Name="statuscode" Type="Edm.String" />
      <Property Name="name" Type="Edm.String" />
    </EntityType>
    """

    option_set_fields = {"vin_disease": {"statuscode"}}
    parser = MetadataParser(target_db="sqlite")
    schemas = parser.parse_metadata_xml(xml, option_set_fields_by_entity=option_set_fields)

    schema = schemas["vin_disease"]
    statuscode_col = next(c for c in schema.columns if c.name == "statuscode")
    name_col = next(c for c in schema.columns if c.name == "name")

    assert statuscode_col.db_type == "INTEGER"  # Option set
    assert name_col.db_type == "TEXT"  # Regular string
```

**2. Integration Tests**

**File**: `tests/integration/test_config_loading.py` (NEW)
```python
import json
import tempfile
from pathlib import Path

async def test_config_file_loading():
    """Schema initializer should load config from file"""
    # Create temp config file
    with tempfile.TemporaryDirectory() as tmpdir:
        config_path = Path(tmpdir) / "optionsets.json"
        config_data = {"vin_disease": ["statuscode", "new_globalhealtharea"]}

        with open(config_path, "w") as f:
            json.dump(config_data, f)

        # Mock initialize_tables to use this config path
        # Assert config is loaded
        # Assert tables are created with INTEGER columns

def test_generate_optionset_config_script():
    """Config generator should correctly extract option sets from database"""
    # Create test database with option set tables
    # Run generate_optionset_config.extract_option_sets()
    # Assert correct mapping of fields to entities
```

**3. End-to-End Tests**

**File**: `tests/e2e/test_full_sync_with_config.py` (NEW)
```python
async def test_sync_with_config_creates_integer_columns():
    """When config exists, option set columns should be INTEGER"""
    # 1. Create config file with known option sets
    # 2. Run full sync from scratch
    # 3. Check database schema
    cursor.execute("SELECT sql FROM sqlite_master WHERE name='vin_diseases'")
    create_sql = cursor.fetchone()[0]

    # Assert columns from config are INTEGER
    assert "new_globalhealtharea INTEGER" in create_sql
    assert "statuscode INTEGER" in create_sql

async def test_sync_without_config_creates_text_columns():
    """Without config, option set columns should be TEXT (current behavior)"""
    # 1. Remove config file if exists
    # 2. Run full sync
    # 3. Check that option set fields are TEXT
    # 4. Run config generator
    # 5. Verify it finds all option sets
```

---

### Migration Considerations

**For Existing Databases**:

Users who have already synced with TEXT columns will need to:

**Option A: Recreate from scratch** (simplest)
```bash
rm dataverse_complete.db
python sync_dataverse.py
```

**Option B: Manual migration** (preserve data)
```sql
-- For each entity with option set TEXT columns:

-- 1. Rename old table
ALTER TABLE vin_diseases RENAME TO vin_diseases_old;

-- 2. Create new table with INTEGER columns (will be done by sync)
-- (run sync with new code)

-- 3. Copy data
INSERT INTO vin_diseases SELECT * FROM vin_diseases_old;

-- 4. Drop old table
DROP TABLE vin_diseases_old;
```

**Recommendation**: Document in CHANGELOG that users should recreate the database after this update.

---

### Performance Impact

**No Additional API Calls**: Approach 6 uses config file - NO extra API requests during normal sync

**One-Time Setup** (only needed once):
1. Initial sync with TEXT columns: ~2-5 minutes (same as current)
2. Run config generator: <1 second (SQLite query, local)
3. Delete database and re-sync with INTEGER columns: ~2-5 minutes (same as current)

**Total one-time setup**: ~5-10 minutes (two full syncs + config generation)

**Ongoing Performance**:
- Config file loaded once at startup: <0.1 seconds
- No performance penalty vs current behavior
- Subsequent syncs: Same speed as current implementation

---

## Implementation Checklist

- [ ] 1. Create `generate_optionset_config.py` script:
  - [ ] Implement `extract_option_sets()` function
  - [ ] Handle plural-to-singular entity name conversion
  - [ ] Output JSON to stdout with helpful instructions
- [ ] 2. Modify `lib/sync/schema_initializer.py` to:
  - [ ] Load config from `config/optionsets.json` if exists
  - [ ] Print warning if config not found
  - [ ] Pass `option_set_fields_by_entity` to schema fetcher
- [ ] 3. Modify `lib/validation/dataverse_schema.py` to:
  - [ ] Accept `option_set_fields_by_entity` parameter in `fetch_schemas_from_metadata()`
  - [ ] Pass it through to parser
- [ ] 4. Modify `lib/validation/metadata_parser.py` to:
  - [ ] Accept `option_set_fields_by_entity` parameter in `parse_metadata_xml()`
  - [ ] Convert list to set and pass to `_parse_entity_type()`
  - [ ] Pass `option_set_fields` to `_parse_properties()`
  - [ ] Pass `is_option_set` flag to `map_edm_to_db_type()`
- [ ] 5. Modify `lib/type_mapping.py` to:
  - [ ] Add `is_option_set` parameter to `map_edm_to_db_type()`
  - [ ] Return INTEGER for option sets (Edm.String + is_option_set=True)
- [ ] 6. Write unit tests:
  - [ ] Type mapping override test
  - [ ] Metadata parser with config test
- [ ] 7. Write integration tests:
  - [ ] Config file loading test
  - [ ] Config generator script test
- [ ] 8. Write e2e tests:
  - [ ] Sync with config creates INTEGER columns
  - [ ] Sync without config creates TEXT columns
- [ ] 9. Test workflow:
  - [ ] Full sync without config (TEXT columns)
  - [ ] Generate config file
  - [ ] Re-sync from scratch with config (INTEGER columns)
  - [ ] Verify schema correctness
- [ ] 10. Update documentation:
  - [ ] README: Document config generation workflow
  - [ ] CHANGELOG: Note breaking change (requires DB recreate)
  - [ ] Add example config file to docs
- [ ] 11. Commit config file to repo (after generating from real sync)

---

## Success Criteria

✅ **Schema Correctness**: Option set columns created as INTEGER, not TEXT
✅ **Data Integrity**: All joins between entity tables and option set lookup tables work correctly
✅ **Automation**: No manual configuration required
✅ **Backward Compatibility**: Full sync from scratch works without errors
✅ **Test Coverage**: All new code covered by unit and integration tests
✅ **Performance**: Sample data fetching adds <10 seconds to sync time

---

## Critical Files

### To Create
- `generate_optionset_config.py` - Config generator script (root directory)
- `config/optionsets.json` - Generated config file (committed to repo)
- `tests/unit/type_mapping/test_optionset_type_override.py` - Unit tests for type override
- `tests/unit/validation/test_metadata_parser_optionsets.py` - Parser integration tests
- `tests/integration/test_config_loading.py` - Config loading tests
- `tests/e2e/test_full_sync_with_config.py` - E2E tests with/without config

### To Modify
- `lib/sync/schema_initializer.py` - Load config file and pass to schema fetcher
- `lib/validation/dataverse_schema.py` - Accept and forward option set fields
- `lib/validation/metadata_parser.py` - Use config to override type mapping
- `lib/type_mapping.py` - Add `is_option_set` parameter to `map_edm_to_db_type()`

### To Update
- `README.md` - Document config generation workflow
- `CHANGELOG.md` - Note breaking change (requires database recreate)
- `.gitignore` - Ensure `config/optionsets.json` is tracked (NOT ignored)

---

## Risks and Mitigation

**Risk 1: Config file out of sync with Dataverse schema**
- **Scenario**: New option sets added to Dataverse but config file not updated
- **Mitigation**: Document when to regenerate config (after schema changes); new fields will be TEXT until config is updated
- **Impact**: Low - joins still work, just not optimal type; easy to fix by regenerating config

**Risk 2: Plural-to-singular conversion errors**
- **Scenario**: Config generator incorrectly converts entity table names to singular forms
- **Mitigation**: Use well-tested conversion heuristics; allow manual config editing if needed
- **Impact**: Low - config is JSON and easily editable by hand

**Risk 3: Manual step required (running config generator)**
- **Scenario**: Users forget to run config generator
- **Mitigation**: Show clear warning during sync if config not found; include instructions in README
- **Impact**: Low - system works without config (TEXT columns), warning reminds user

**Risk 4: Breaking change for existing databases**
- **Scenario**: Users upgrade and want INTEGER columns but have TEXT
- **Mitigation**: Document workflow clearly: generate config, delete DB, re-sync
- **Impact**: Medium - requires full re-sync (~5-10 minutes), but only once

---

## Alternative Approaches Not Chosen

1. **Approach 1 - Configuration-Driven (Manual)**: Rejected - requires manually identifying 70+ option set fields, maintenance burden
2. **Approach 2 - ALTER TABLE Post-Detection**: Rejected - complex CREATE-COPY-DROP pattern, migration risk, no existing capability
3. **Approach 3 - Deferred Schema Creation**: Rejected - major architectural refactor, where to store data before tables exist?
4. **Approach 4 - Sample Data Pre-Analysis**: **Rejected - FATAL FLAW** - doesn't work with sparse data (NULL values have no FormattedValue annotation)
5. **Approach 5 - Metadata Injection**: Rejected - same sparse data problem as Approach 4, fragile XML manipulation
6. **Approach 7 - ALTER TABLE (Automatic)**: Viable but more complex than Approach 6; requires migration logic, transaction handling, foreign key management

---

## Next Steps

Once approved, implement in this order:

1. **Type mapper changes** (`lib/type_mapping.py`) - Add `is_option_set` parameter
2. **Metadata parser changes** (`lib/validation/metadata_parser.py`) - Accept config and pass through
3. **Schema fetcher changes** (`lib/validation/dataverse_schema.py`) - Forward config to parser
4. **Schema initializer changes** (`lib/sync/schema_initializer.py`) - Load config file
5. **Config generator script** (`generate_optionset_config.py`) - Create extraction logic
6. **Unit tests** - Type mapping and parser integration
7. **Integration tests** - Config loading and generator
8. **E2E tests** - Full workflow with/without config
9. **Documentation** - README, CHANGELOG, workflow instructions
10. **Test workflow**:
    - Sync without config (verify TEXT columns)
    - Generate config file
    - Re-sync from scratch (verify INTEGER columns)
    - Commit config file to repo

**User Workflow After Implementation**:
```bash
# One-time setup (if starting fresh or after Dataverse schema changes):
python sync_dataverse.py                           # Sync with TEXT columns
python generate_optionset_config.py > config/optionsets.json  # Generate config
git add config/optionsets.json && git commit       # Commit config
rm dataverse_complete.db                            # Delete old database
python sync_dataverse.py                            # Re-sync with INTEGER columns

# Ongoing syncs (config already exists):
python sync_dataverse.py                            # Just run normally
```
