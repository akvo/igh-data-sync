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

The sync tool follows an 8-step workflow with integrated validation:

1. **Load Configuration** - Read credentials and entity list (with api_name mapping)
2. **Authenticate** - OAuth with auto-refresh (50-min window before expiry)
3. **Validate Schema** - Compare $metadata against database, exit on breaking changes
4. **Initialize Database** - Create missing tables and sync tracking tables
5. **Build Relationship Graph** - Extract foreign key relationships from $metadata
6. **Sync Entities** - Sync in two phases:
   - **Unfiltered entities**: Full incremental sync with pagination
   - **Filtered entities**: Transitive closure sync (only referenced records)
7. **Verify References** - (Optional with --verify) Check for dangling foreign keys
8. **Summary** - Report total records added/updated across all entities

### Filtered Entity Sync

Filtered entities use **transitive closure** to sync only records referenced by already-synced entities, minimizing data transfer.

**Algorithm:**
1. Sync all unfiltered entities first
2. For each filtered entity:
   - Extract referenced IDs from foreign keys in already-synced tables
   - Build `$filter` query: `accountid eq 'a1' or accountid eq 'a2' or ...`
   - Fetch only those records from Dataverse
3. Repeat until convergence (no new IDs found)

**Example:**

Given this configuration:
```json
{
  "entities": [
    {
      "name": "vin_candidate",
      "api_name": "vin_candidates",
      "filtered": false
    },
    {
      "name": "account",
      "api_name": "accounts",
      "filtered": true
    }
  ]
}
```

**Sync behavior:**
1. Sync **all** vin_candidates (2,000 records)
2. Extract account IDs from `vin_candidates._parentaccountid_value` (finds 150 unique IDs)
3. Sync **only those 150 accounts** instead of all 50,000 accounts in Dataverse

**Benefits:**
- 📉 Minimal data transfer (sync 150 accounts instead of 50,000)
- 🔗 Maintains referential integrity (no dangling FKs)
- ♻️ Supports transitive dependencies (account → systemuser → team)

## Project Structure

```
 igh-clean-sync/
├── validate_schema.py          # Schema validator entrypoint
├── sync_dataverse.py            # Sync tool entrypoint
├── entities_config.json         # Entity configuration with filtered sync
├── .env                         # Credentials (not committed)
├── .env.example                 # Template
├── requirements.txt             # Runtime dependencies
├── requirements-dev.txt         # Development dependencies (NEW)
├── pyproject.toml               # Project config (pytest, ruff, pylint) (NEW)
├── .pre-commit-config.yaml      # Pre-commit hooks configuration (NEW)
├── lib/                         # Reusable utilities
│   ├── auth.py                  # OAuth authentication with auto-refresh
│   ├── dataverse_client.py      # Async HTTP client with retry/pagination
│   ├── config.py                # Configuration loading with entity mapping
│   ├── type_mapping.py          # Data structures and type mappings
│   ├── validation/              # Validation components
│   │   ├── metadata_parser.py   # Parse $metadata XML
│   │   ├── dataverse_schema.py  # Fetch schemas from Dataverse
│   │   ├── database_schema.py   # Query database schemas
│   │   ├── schema_comparer.py   # Compare and detect differences
│   │   ├── report_generator.py  # Generate JSON/Markdown reports
│   │   └── validator.py         # Pre-sync validation workflow (NEW)
│   └── sync/                    # Sync components
│       ├── schema_initializer.py # Create tables from $metadata schemas
│       ├── database.py          # SQLite operations (UPSERT, tracking)
│       ├── sync_state.py        # Sync state management
│       ├── entity_sync.py       # Individual entity sync logic (NEW)
│       ├── filtered_sync.py     # Filtered entity transitive closure (NEW)
│       ├── reference_verifier.py # FK integrity verification (NEW)
│       └── relationship_graph.py # FK relationship graph (NEW)
└── tests/                       # Test suite (67 tests, 60.20% coverage)
    ├── test_auth.py             # OAuth authentication tests (NEW)
    ├── test_dataverse_client.py # API client tests (NEW)
    ├── test_integration_sync.py # True E2E sync tests (NEW)
    ├── test_validator.py        # Pre-sync validation tests (NEW)
    ├── test_type_mapping.py     # Type mapping tests
    ├── test_metadata_parser.py  # XML parsing tests
    ├── test_schema_comparer.py  # Schema comparison tests
    ├── test_config.py           # Configuration tests
    ├── test_database.py         # Database operations tests
    └── helpers/                 # Test utilities (NEW)
        ├── __init__.py
        └── fake_dataverse_client.py # Mock API client for E2E tests
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

## Development Setup

### Install Development Dependencies

```bash
# Install dev dependencies (includes test, lint, format tools)
pip install -r requirements-dev.txt
```

**Development tools included:**
- `pytest` - Test framework with async support
- `pytest-cov` - Code coverage reporting
- `pytest-mock` - Mocking utilities
- `ruff` - Fast Python linter and formatter
- `pylint` - Additional linting
- `pre-commit` - Git hooks for code quality
- `aioresponses` - Mock async HTTP requests

### Pre-commit Hooks

Install pre-commit hooks to automatically check code quality before commits:

```bash
# Install hooks
pre-commit install

# Run manually on all files
pre-commit run --all-files
```

**Hooks configured:**
- Trailing whitespace removal
- YAML validation
- Large file detection
- Ruff linting and formatting
- Pylint code analysis

### Running Tests

```bash
# Run all tests
pytest

# Run with coverage report
pytest --cov=lib --cov-report=term-missing

# Run specific test file
pytest tests/test_auth.py -v

# Run tests matching pattern
pytest -k "test_filtered_sync"
```

**Test Statistics:**
- 67 tests total
- 60.20% code coverage
- All tests passing

### Linting and Formatting

```bash
# Check code style (ruff)
ruff check lib/ tests/

# Auto-fix issues
ruff check --fix lib/ tests/

# Format code
ruff format lib/ tests/

# Run pylint
pylint lib/ tests/
```

### Type Checking

```bash
# Run type checker (if mypy is added)
mypy lib/
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

### Synced vs Excluded Entities

The sync configuration includes **26 entities** organized into two categories:

**Business entities (22 unfiltered):**
- VIN domain entities: vin_candidate, vin_product, vin_disease, vin_clinicaltrial, vin_capparameter, vin_routeofadministration, etc.
- Junction tables: candidate-account, candidate-systemuser, clinicaltrial-account, etc.

**Filtered entities (4 filtered):**
- Core CRM: account, contact, systemuser
- Metadata: transactioncurrency

**Excluded entities (not synced):**

The following Dataverse entities are **intentionally excluded** from sync:

**System/Infrastructure tables (18):**
- `businessunit`, `organization`, `team`, `businessunitnewsarticle`
- `importfile`, `importlog`, `importmap`, `transformationmapping`
- `pluginassembly`, `plugintype`, `sdkmessage`, `sdkmessageprocessingstep`
- `workflow`, `asyncoperation`, `bulkdeleteoperation`, `bulkdeleteoutput`
- `duplicaterule`, `duplicateruledetection`

**Identity/User Management (10):**
- `systemuserroles`, `systemuserprofiles`, `userentityinstancedata`
- `principal`, `principalobjectaccess`, `principalentitymap`
- `role`, `roleprivileges`, `privilege`, `fieldpermission`

**Address/Metadata (3):**
- `customeraddress` (generic address entity)
- `knowledgearticle`, `knowledgearticleviews`

**Portal/Web (1):**
- `adx_*` tables (Power Pages/Portal entities)

**Rationale for exclusion:**
- **System tables**: Infrastructure entities not relevant to business data
- **Identity management**: User roles/permissions managed by Dataverse
- **Portal entities**: Not applicable to this integration
- **Generic addresses**: Using specific address1/2/3 entities instead

These entities were identified by analyzing foreign key references in the synced entities against the full Dataverse $metadata. They can be added to `entities_config.json` if needed in the future.

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

### Reference Verification

Verify foreign key integrity after sync to ensure the synced subset is self-contained:

```bash
# Normal sync
python sync_dataverse.py

# Sync with reference verification
python sync_dataverse.py --verify
```

**What it does:**
- Runs **after sync completes** as a post-sync validation step
- Uses LEFT JOIN queries to detect dangling foreign keys
- Checks: records that reference other records that don't exist
- **Exits with code 1** if integrity issues found

**When to use:**
- **Filtered entity sync** - Ensures transitive closure captured all dependencies
- **Production validation** - Verify synced subset is internally consistent
- **CI/CD pipelines** - Catch incomplete sync logic

**Example output:**
```
[7/7] Verifying references...

============================================================
Reference Verification Report
============================================================

✗ vin_candidates._parentaccountid_value → accounts: 2 dangling (100 checked)
  Missing IDs: ['acc-123', 'acc-456']

Summary: 1 table(s) with issues, 2 dangling references total
============================================================

❌ SYNC FAILED: Reference integrity issues found
```

**Note:** Verification runs on the **synced SQLite database**, not Dataverse. It validates that your local subset is internally consistent.

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

### Run All Tests

```bash
# Activate virtualenv
source .venv/bin/activate

# Run full test suite
pytest

# Run with verbose output
pytest -v

# Run with coverage
pytest --cov=lib --cov-report=term-missing tests/
```

### Test Coverage

Current coverage: **60.20%** (67 tests passing)

**Coverage by module:**
- lib/auth.py: 96.77%
- lib/dataverse_client.py: 50.00%
- lib/sync/database.py: 90.99%
- lib/sync/entity_sync.py: 57.50%
- lib/sync/filtered_sync.py: 63.24%
- lib/validation/validator.py: 84.51%
- lib/type_mapping.py: 84.62%

**Test types:**
- **Unit tests**: Individual component testing (test_auth.py, test_config.py, etc.)
- **E2E tests**: Full workflow testing with mocked APIs (test_integration_sync.py)
  - Complete sync workflow
  - Incremental sync
  - Filtered sync with transitive closure
  - Empty entity handling

### Run Specific Tests

```bash
# Run single test file
pytest tests/test_auth.py -v

# Run specific test
pytest tests/test_integration_sync.py::TestE2ESync::test_filtered_sync_transitive_closure -v

# Run tests matching pattern
pytest -k "filtered" -v
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
- ✅ Filtered entity sync with transitive closure
- ✅ Reference verification (--verify flag)
- ✅ Pagination with @odata.nextLink
- ✅ Retry with exponential backoff (1-16s)
- ✅ Rate limiting (429) with Retry-After handling
- ✅ Concurrency control (50 parallel requests)
- ✅ Full JSON storage for schema evolution
- ✅ Sync state tracking (_sync_state, _sync_log)
- ✅ UPSERT operations (INSERT OR REPLACE)
- ✅ 67 tests (60.20% coverage), all passing
- ✅ Pre-commit hooks (ruff, pylint)
- ✅ Type checking and linting

## License

[Add your license here]
