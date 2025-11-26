# Dataverse Schema Validator & Sync

A comprehensive toolkit for Microsoft Dataverse that validates schemas and syncs data to SQLite using authoritative $metadata XML for type-accurate table creation.

## Features

### Schema Validator
- ✅ Fetches complete OData $metadata XML from Dataverse (~7 MB, 800+ entities)
- ✅ Parses entity schemas with columns, primary keys, and foreign keys
- ✅ Supports both SQLite and PostgreSQL databases
- ✅ Detects missing tables, columns, and type mismatches
- ✅ Generates JSON and Markdown validation reports
- ✅ Returns exit code 0 (passed) or 1 (failed) for CI/CD integration

### Sync Tool
- ✅ Integrated schema validation before each sync
- ✅ Authoritative schema from $metadata (not inferred)
- ✅ Auto-refresh authentication
- ✅ Retry with exponential backoff
- ✅ 429 rate limit handling
- ✅ Pagination via @odata.nextLink
- ✅ Concurrency control (50 parallel requests)
- ✅ Incremental sync using modifiedon timestamps
- ✅ Full JSON storage for schema evolution
- ✅ Sync state tracking and resumability

## Architecture

### Schema Validator Workflow

The validator follows a clean 6-step workflow:

1. **Load Configuration** - Read credentials and entity list
2. **Authenticate** - OAuth authentication with automatic tenant discovery
3. **Fetch Dataverse Schemas** - Parse $metadata XML for authoritative schemas
4. **Query Database Schemas** - Extract schemas from SQLite/PostgreSQL
5. **Compare Schemas** - Detect differences (errors, warnings, info)
6. **Generate Reports** - Create JSON, Markdown reports and console summary

### Sync Workflow

The sync tool follows a 7-step workflow with integrated validation:

1. **Load Configuration** - Read credentials and entity list (with api_name mapping)
2. **Authenticate** - OAuth with auto-refresh (50-min window before expiry)
3. **Validate Schema** - Compare $metadata against database, exit on breaking changes
4. **Initialize Database** - Create missing tables and sync tracking tables
5. **Prepare for Sync** - Fetch schemas from $metadata for all valid entities
6. **Sync Entities** - Incremental sync with pagination, retry, and concurrency control
7. **Summary** - Report total records added/updated across all entities

## Project Structure

```
igh-clean-sync/
├── validate_schema.py          # Schema validator entrypoint
├── sync_dataverse.py            # Sync tool entrypoint
├── entities_config.json         # Entity configuration (23 entities with api_name)
├── .env                         # Credentials (not committed)
├── .env.example                 # Template
├── requirements.txt             # Dependencies
├── lib/                         # Reusable utilities
│   ├── auth.py                  # OAuth authentication with auto-refresh
│   ├── dataverse_client.py      # Async HTTP client with retry/pagination
│   ├── config.py                # Configuration loading with entity mapping
│   ├── type_mapping.py          # Data structures and type mappings
│   ├── validation/              # Validation components
│   │   ├── metadata_parser.py   # Parse $metadata XML (KEY COMPONENT)
│   │   ├── dataverse_schema.py  # Fetch schemas from Dataverse
│   │   ├── database_schema.py   # Query database schemas
│   │   ├── schema_comparer.py   # Compare and detect differences
│   │   └── report_generator.py  # Generate reports
│   └── sync/                    # Sync components
│       ├── schema_initializer.py # Create tables from $metadata schemas
│       ├── database.py          # SQLite operations (UPSERT, tracking)
│       └── sync_state.py        # Sync state management
└── tests/                       # Unit tests (36 tests, all passing)
    ├── test_type_mapping.py
    ├── test_metadata_parser.py
    ├── test_schema_comparer.py
    ├── test_config.py           # Entity name mapping tests
    └── test_database.py         # Database operations tests
```

## Installation

```bash
# Create virtual environment
python3 -m venv .venv

# Activate virtual environment
# On Linux/macOS:
source .venv/bin/activate
# On Windows:
# .venv\Scripts\activate

# Install dependencies
pip install -r requirements.txt

# Copy environment template
cp .env.example .env

# Edit .env with your credentials
vim .env
```

## Configuration

### .env file
```bash
# Dataverse API Configuration
DATAVERSE_API_URL=https://your-org.api.crm.dynamics.com/api/data/v9.2/
DATAVERSE_CLIENT_ID=your-client-id-here
DATAVERSE_CLIENT_SECRET=your-client-secret-here
DATAVERSE_SCOPE=https://your-org.api.crm.dynamics.com/.default

# Database Configuration
SQLITE_DB_PATH=../dataverse_complete.db
# Or for PostgreSQL:
# POSTGRES_CONNECTION_STRING=postgresql://user:password@localhost:5432/dbname
```

### entities_config.json

Lists entities to sync/validate with singular/plural name mapping:
```json
{
  "entities": [
    {
      "name": "vin_candidate",
      "api_name": "vin_candidates",
      "filtered": false,
      "description": "All candidate records"
    },
    {
      "name": "account",
      "api_name": "accounts",
      "filtered": true,
      "description": "Filtered accounts linked via junction tables"
    }
  ]
}
```

**Field descriptions:**
- `name`: Singular entity name used in $metadata XML lookups
- `api_name`: Plural name used for API endpoints and table names
- `filtered`: If true, sync only records linked to already-synced entities
- `description`: Human-readable description

## Usage

### Schema Validation

Validate database schema against Dataverse $metadata:

```bash
# Activate virtualenv first
source .venv/bin/activate

# Basic validation (auto-detect database type)
python validate_schema.py

# Specify database type
python validate_schema.py --db-type sqlite
python validate_schema.py --db-type postgresql

# Custom report paths
python validate_schema.py \
  --json-report reports/schema.json \
  --md-report reports/schema.md
```

### Data Sync

Sync Dataverse entities to SQLite with integrated schema validation:

```bash
# Activate virtualenv first
source .venv/bin/activate

# Run sync
python sync_dataverse.py
```

The sync tool automatically:
1. Validates schema before syncing (exits on breaking changes)
2. Creates missing tables from $metadata schemas
3. Performs incremental sync using `modifiedon` timestamps
4. Handles rate limiting (429) and retries failed requests
5. Tracks sync state in `_sync_state` and `_sync_log` tables
6. Stores full JSON responses for schema evolution

**Output example:**
```
============================================================
DATAVERSE TO SQLITE SYNC
============================================================

[1/7] Loading configuration...
  ✓ Loaded config for 23 entities
  ✓ Database: ../dataverse_complete.db

[2/7] Authenticating...
  ✓ Authenticated (tenant: abc123...)

[3/7] Validating schema...
  Fetching schemas from Dataverse $metadata...

  Schema Validation Results:
    Errors: 0, Warnings: 0, Info: 2

    ℹ️  INFO [new_entity]: New entity - table will be created

  ✓ Validation passed with 0 warning(s), 2 info

[4/7] Initializing database...
  ✓ Sync tables initialized
  ✓ Created table: new_entity

[5/7] Preparing for sync...
  ✓ Schemas loaded for 23 entities

[6/7] Syncing data...
  Syncing vin_candidates...
  ✓ vin_candidates: 150 added, 23 updated
  ...

[7/7] Sync complete!
============================================================
Total records added: 1523
Total records updated: 347
============================================================
```

## Output

### Console Summary
```
============================================================
SCHEMA VALIDATION SUMMARY
============================================================
Entities checked: 23
Total issues: 5
  - Errors:   2
  - Warnings: 3
  - Info:     0
============================================================
✅ VALIDATION PASSED - No critical errors
============================================================
```

### Reports Generated

1. **schema_validation_report.json** - Machine-readable JSON with all differences
2. **schema_validation_report.md** - Human-readable Markdown report

## Testing

Run the test suite (36 tests):
```bash
# Activate virtualenv
source .venv/bin/activate

# Run tests
python -m unittest discover tests/ -v
```

## Schema Change Handling

The sync tool validates schemas before each sync and handles changes automatically:

| Schema Change | Severity | Behavior |
|--------------|----------|----------|
| **Type mismatch** (e.g., INT → TEXT) | ERROR | Alert and exit sync (prevents data corruption) |
| **Primary key change** | ERROR | Alert and exit sync (breaking change) |
| **Missing table** (new entity in config) | INFO | Alert and auto-create table from $metadata |
| **New column** in Dataverse | INFO | Alert and continue (stored in `json_response`) |
| **Removed column** from Dataverse | INFO | Alert and continue (column remains in DB) |
| **Entity in config but not in $metadata** | WARNING | Alert and skip entity |

**Design rationale:**
- **Exit on type/PK changes**: Ensures PostgreSQL compatibility (no flexible typing)
- **Auto-create tables**: New entities are expected when config is updated
- **No ALTER TABLE**: New columns stored in `json_response` for schema evolution
- **Keep removed columns**: Preserves historical data, no destructive changes
- **No interactivity**: All decisions automated for CI/CD compatibility

## Design Decisions

### Why $metadata XML?

**Authoritative** - Official OData CSDL schema, not inferred from data
**Complete** - ALL columns, even if null in sample data
**Type-accurate** - Exact Edm types (Int32 vs Decimal, String vs Memo)
**Non-circular** - Schema source independent of validation target
**Includes relationships** - Foreign keys via NavigationProperty elements

### Alternative Approaches (NOT used)

- ❌ **EntityDefinitions API** - Doesn't work for all entities (returns 400 errors)
- ❌ **Sample data inference** - Circular logic, misses null columns

## Development

### Adding New Validation Rules

1. Update `schema_comparer.py` with new comparison logic
2. Add corresponding tests in `tests/test_schema_comparer.py`
3. Update report generation if needed

### Supporting Additional Databases

1. Add type mappings in `type_mapping.py`
2. Implement schema query in `database_schema.py`
3. Add tests for new database type

## CI/CD Integration

The validator returns appropriate exit codes:
- **0** - Validation passed (no critical errors)
- **1** - Validation failed (critical errors detected)

Example CI workflow:
```yaml
- name: Validate Schema
  run: |
    python3 validate_schema.py --db-type sqlite
```

## Success Criteria

### Schema Validator
- ✅ Fetches $metadata XML (~7 MB, 800+ entities)
- ✅ Extracts all requested entity schemas
- ✅ Detects missing tables in database
- ✅ Detects column differences (missing, extra, type mismatches)
- ✅ Detects primary key differences
- ✅ Detects foreign key differences
- ✅ Generates JSON and Markdown reports
- ✅ Exit code 0 = passed, 1 = failed

### Sync Tool
- ✅ Integrated schema validation before each sync
- ✅ Auto-creates tables from authoritative $metadata schemas
- ✅ Exits on breaking schema changes (type/PK mismatches)
- ✅ Incremental sync using modifiedon timestamps
- ✅ Pagination with @odata.nextLink
- ✅ Retry with exponential backoff (1-16s)
- ✅ Rate limiting (429) with Retry-After handling
- ✅ Concurrency control (50 parallel requests)
- ✅ Full JSON storage for schema evolution
- ✅ Sync state tracking (_sync_state, _sync_log)
- ✅ UPSERT operations (INSERT OR REPLACE)
- ✅ 36 unit tests, all passing

## License

[Add your license here]
