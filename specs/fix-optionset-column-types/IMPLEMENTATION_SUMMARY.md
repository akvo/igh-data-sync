# Implementation Summary: Option Set Column Type Fix

**Status:** ✅ Complete
**Date:** 2025-01-08
**Approach:** Configuration-Driven with Auto-Generation (Approach 6)

## Problem Statement

Option set fields in Dataverse (enumerated values like `statuscode`, `new_globalhealtharea`) were being created as **TEXT** columns instead of **INTEGER** columns, even though they store integer codes and should have proper foreign key relationships to option set lookup tables.

### Root Cause

Dataverse's $metadata XML represents option set fields as `Edm.String`, making them indistinguishable from regular text fields during schema creation. Option sets are only detected during data sync when the API returns `@OData.Community.Display.V1.FormattedValue` annotations.

**Timeline Issue:**
1. Tables created from $metadata (Step 4) → Option sets appear as Edm.String → Created as TEXT
2. Option sets detected from data (Step 6) → Too late, tables already exist

## Solution Overview

Implemented **Approach 6: Configuration-Driven with Auto-Generation**

### Two-Phase Workflow

**Phase 1: Initial Discovery (run once)**
```bash
python sync_dataverse.py                           # Sync with TEXT columns
python generate_optionset_config.py > config/optionsets.json  # Generate config
rm dataverse_complete.db                            # Delete database
python sync_dataverse.py                            # Re-sync with INTEGER columns
```

**Phase 2: Ongoing Use**
```bash
python sync_dataverse.py  # Uses config/optionsets.json automatically
```

### Why This Approach?

After evaluating 7 approaches, we selected this one because:
- ✅ **Reliable with sparse data** - User reported "many records have no value for their option set columns", which ruled out sample-based detection
- ✅ **Simple implementation** - Config file + type mapping override
- ✅ **Maintainable** - Config committed to repo, survives DB recreates
- ✅ **No API overhead** - Config loaded once at startup

## Implementation Details

### Files Created

**1. `generate_optionset_config.py` (121 lines)**
- Analyzes synced database to find all `_optionset_*` lookup tables
- Maps option set fields back to entity names
- Outputs JSON configuration to stdout

**Key Function:**
```python
def extract_option_sets(db_path: str) -> dict[str, list[str]]:
    # Queries _optionset_* tables
    # Maps fields to entities via PRAGMA table_info
    # Handles plural → singular conversion
    return {"vin_disease": ["statuscode", "new_globalhealtharea"]}
```

**2. `config/optionsets.json`**
- Maps entity names to lists of option set field names
- Committed to repository
- Example:
```json
{
  "vin_disease": [
    "new_globalhealtharea",
    "statuscode",
    "statecode",
    "vin_type"
  ]
}
```

### Files Modified

**1. `lib/type_mapping.py` (Lines 115-158)**
- Added `is_option_set` parameter to `map_edm_to_db_type()`
- Returns `INTEGER` when `is_option_set=True` and `edm_type="Edm.String"`

```python
def map_edm_to_db_type(
    edm_type: str,
    target_db: str,
    max_length: Optional[int] = None,
    is_option_set: bool = False,  # NEW
) -> str:
    # Override for option sets
    if is_option_set and edm_type == "Edm.String":
        return "INTEGER"
    # ... normal type mapping
```

**2. `lib/validation/metadata_parser.py` (Lines 24-197)**
- Modified 3 methods to accept and forward `option_set_fields_by_entity` config
- `parse_metadata_xml()` - Accepts config dict, extracts per-entity fields
- `_parse_entity_type()` - Passes option set fields through pipeline
- `_parse_properties()` - Checks if field is option set, passes flag to type mapper

**3. `lib/validation/dataverse_schema.py` (Lines 28-64)**
- Added `option_set_fields_by_entity` parameter to `fetch_schemas_from_metadata()`
- Forwards config to metadata parser

**4. `lib/sync/schema_initializer.py` (Lines 60-98)**
- Loads `config/optionsets.json` if exists
- Shows helpful warning if config not found
- Passes config to schema fetcher

```python
config_path = Path("config/optionsets.json")
if config_path.exists():
    with open(config_path, encoding="utf-8") as f:
        option_set_fields_by_entity = json.load(f)
    # Pass to schema fetcher
else:
    print("⚠️  No option set config found - tables will use TEXT for option sets")
    print(f"   To fix: Run sync, then: python generate_optionset_config.py > {config_path}")
```

### Tests Added

**1. Unit Tests (14 tests total)**
- `tests/unit/test_type_mapping_optionset.py` (8 tests)
  - Type override behavior (Edm.String → INTEGER when is_option_set=True)
  - PostgreSQL support
  - Max_length handling
  - Other types not affected

- `tests/unit/validation/test_metadata_parser_optionsets.py` (6 tests)
  - Parser with/without config
  - Partial config (some entities configured, others not)
  - Empty config
  - PostgreSQL type mapping

**2. End-to-End Tests (5 tests)**
- `tests/e2e/test_optionset_config_workflow.py` (300+ lines, 5 tests)
  - Schema creation without config → TEXT columns
  - Schema creation with config → INTEGER columns
  - Multi-entity config handling
  - Informative messages for user (warnings, confirmations)
  - Config loading workflow

### Test Results

```
97 tests passing (19 new + 78 existing)
63.13% code coverage (exceeds 38% requirement)
All linting clean on new/modified files
```

## Technical Architecture

### Type Mapping Pipeline

```
$metadata XML
    ↓
Parse entities (metadata_parser.py)
    ↓
Check if field in config → is_option_set flag
    ↓
map_edm_to_db_type(edm_type, target_db, is_option_set)
    ↓
    ├─ is_option_set=True & Edm.String → INTEGER
    └─ Normal type mapping → TEXT/VARCHAR/etc
    ↓
CREATE TABLE with correct types
```

### Config Loading

```
schema_initializer.py startup:
1. Check if config/optionsets.json exists
2. If yes: Load and parse JSON → dict[str, list[str]]
3. Pass to DataverseSchemaFetcher
4. Pass to MetadataParser
5. Use during type mapping
```

### Config Generation

```
generate_optionset_config.py:
1. Connect to synced database
2. Find all tables LIKE '_optionset_%'
3. For each option set table:
   a. Extract field name (e.g., _optionset_statuscode → statuscode)
   b. Query all entity tables (PRAGMA table_info)
   c. Check which entities have this field
   d. Convert plural table name → singular entity name
4. Output JSON: {"entity": ["field1", "field2"]}
```

## User Workflow

### First-Time Setup (After First Sync)

```bash
# 1. Initial sync creates TEXT columns and detects option sets
python sync_dataverse.py

# 2. Generate config from detected option sets
mkdir -p config
python generate_optionset_config.py > config/optionsets.json

# 3. Verify config looks correct
cat config/optionsets.json

# 4. Commit config to repository
git add config/optionsets.json
git commit -m "Add option set configuration"

# 5. Delete database and re-sync with INTEGER columns
rm dataverse_complete.db
python sync_dataverse.py
```

### Normal Usage (After Config Exists)

```bash
# Just run sync - config loaded automatically
python sync_dataverse.py
```

Output shows:
```
Loading option set configuration from config/optionsets.json...
  ✓ Loaded config for 1 entities, 4 option set fields
```

### When to Regenerate Config

Regenerate `config/optionsets.json` when:
- New entities added to `entities_config.json`
- New option set fields added to existing entities in Dataverse
- Dataverse schema changes (rare)

## Benefits

1. **Correct Schema** - Option set columns are INTEGER, enabling proper foreign keys
2. **Better Queries** - Can join to `_optionset_*` tables with INTEGER keys
3. **Data Integrity** - Type enforcement prevents invalid values
4. **PostgreSQL Compatible** - INTEGER columns work correctly (vs TEXT)
5. **Automatic** - Config loaded transparently during sync

## Limitations & Known Issues

### 1. Manual Step Required
**Issue:** Users must run `generate_optionset_config.py` after first sync
**Mitigation:** Clear warnings during sync, documentation in README
**Impact:** Low - one-time setup, well-documented

### 2. Config Maintenance
**Issue:** Config can become stale if Dataverse schema changes
**Mitigation:** Regenerate config when adding entities or seeing new option sets
**Impact:** Low - Dataverse schema changes are rare

### 3. Breaking Change for Existing Databases
**Issue:** Existing databases have TEXT columns, new databases have INTEGER
**Mitigation:** Document migration path (delete DB and re-sync)
**Impact:** Medium - requires full re-sync (~5-10 minutes)

### 4. Plural-to-Singular Conversion
**Issue:** Config generator uses heuristics for table name conversion
**Mitigation:** Simple rules handle 99% of cases; config is JSON and manually editable
**Impact:** Low - easy to fix by hand if needed

## Alternative Approaches Considered

### Rejected Approaches

1. **Sample Data Pre-Analysis** (Approach 4)
   - **Why rejected:** User reported sparse data - many NULL values lack FormattedValue annotations
   - FormattedValue only present when value is non-NULL

2. **ALTER TABLE After Detection** (Approach 2)
   - **Why rejected:** Complex CREATE-COPY-DROP pattern, migration risk, no existing capability

3. **Deferred Schema Creation** (Approach 3)
   - **Why rejected:** Major architectural refactor, where to store data before tables exist?

4. **Manual Configuration** (Approach 1)
   - **Why rejected:** 70+ option set fields across 26 entities, maintenance burden

5. **Metadata Injection** (Approach 5)
   - **Why rejected:** Same sparse data problem as Approach 4, fragile XML manipulation

6. **Automatic ALTER TABLE** (Approach 7)
   - **Why viable but not chosen:** More complex than Approach 6, migration logic risky

**Full evaluation in:** `specs/fix-optionset-column-types/plan.md`

## Database Schema Impact

### Before (TEXT Columns)
```sql
CREATE TABLE vin_diseases (
  vin_diseaseid TEXT PRIMARY KEY,
  new_globalhealtharea TEXT,  -- ❌ Wrong type
  statuscode TEXT,             -- ❌ Wrong type
  vin_name TEXT
);

-- Joins work but semantically incorrect (SQLite implicit conversion)
SELECT d.*, o.label
FROM vin_diseases d
LEFT JOIN _optionset_new_globalhealtharea o
  ON d.new_globalhealtharea = o.code;  -- TEXT = INTEGER (works but wrong)
```

### After (INTEGER Columns)
```sql
CREATE TABLE vin_diseases (
  vin_diseaseid TEXT PRIMARY KEY,
  new_globalhealtharea INTEGER,  -- ✅ Correct type
  statuscode INTEGER,              -- ✅ Correct type
  vin_name TEXT,
  FOREIGN KEY (new_globalhealtharea)
    REFERENCES _optionset_new_globalhealtharea(code)
);

-- Proper typed join
SELECT d.*, o.label
FROM vin_diseases d
LEFT JOIN _optionset_new_globalhealtharea o
  ON d.new_globalhealtharea = o.code;  -- INTEGER = INTEGER ✅
```

## Code Quality

### Linting Status
- ✅ All new/modified files pass ruff checks
- ⚠️ 83 pre-existing linting issues in lib/ (not introduced by this change)
- Issues fixed: encoding in open(), line length, unused variables, imports location

### Test Coverage
- **New code:** Well-tested (19 new tests covering all features)
- **Overall coverage:** 63.13% (slight increase from adding new tested code)
- **Existing untested code:** Not addressed (out of scope for this feature)

## Performance Impact

- **No additional API calls** - Config loaded from file, no network overhead
- **One-time setup cost** - Config generation <1 second (local SQLite query)
- **Ongoing overhead** - Config loaded once at startup: <0.1 seconds
- **Sync performance** - No change (type mapping happens during schema creation, not sync)

## Future Enhancements

### Possible Improvements
1. **Automatic config generation** - Generate config on first sync automatically
2. **Config validation** - Warn if config references non-existent fields
3. **Migration tool** - Script to ALTER existing databases (TEXT → INTEGER)
4. **UI feedback** - Show which fields are using option set override during sync

### Not Planned
- **Detecting option sets from $metadata alone** - Not possible (appear as Edm.String)
- **Real-time config updates** - Config is static, loaded at startup

## References

- **Full Plan:** `specs/fix-optionset-column-types/plan.md`
- **Generated Config:** `config/optionsets.json`
- **Config Generator:** `generate_optionset_config.py`
- **Type Mapping:** `lib/type_mapping.py:115-158`
- **Tests:** `tests/unit/test_type_mapping_optionset.py`, `tests/unit/validation/test_metadata_parser_optionsets.py`, `tests/e2e/test_optionset_config_workflow.py`
