# Dataverse to SQLite Sync Reimplementation Plan

## Overview

Reimplement the sync script from scratch with clean architecture, comprehensive testing, and reusable utilities. The new implementation will support idempotent syncs, schema evolution, filtered entities, and complete API response preservation.

## Key Design Decisions

Based on user requirements:
- **Entity names**: Use API plural names (vin_candidates, accounts, etc.)
- **JSON storage**: Store complete API response including @odata metadata
- **Filtering strategy**: Two-phase sync - unfiltered entities first, then accounts/contacts with $filter queries
- **valid_from**: Use Dataverse's modifiedon timestamp
- **Schema evolution**: Lock schema after first sync, alert on changes, preserve new fields in JSON column
- **Sync state storage**: Store in target SQLite database (supports Airflow deployment where orchestrator is stateless)
- **Clock skew**: Not needed - use exact timestamps from Dataverse responses
- **Database support**: SQLite only (PostgreSQL support can be added later if needed)

## Project Structure

```
clean/
├── sync_dataverse.py              # Main entrypoint (~250 lines)
├── entities_config.json           # Entity configuration (update to plurals)
├── requirements.txt
├── .env / .env.example
│
├── lib/                           # Reusable utilities
│   ├── auth.py                    # OAuth token management (~80 lines)
│   ├── config.py                  # Configuration loading (~80 lines)
│   ├── dataverse_client.py        # HTTP client with retry/pagination (~180 lines)
│   ├── schema_manager.py          # Schema discovery/validation (~250 lines)
│   ├── database.py                # SQLite operations (~200 lines)
│   └── sync_state.py              # Sync state tracking (~120 lines)
│
├── tests/
│   ├── test_*.py                  # Unit tests for each module
│   ├── test_integration_*.py      # Integration tests
│   └── fixtures/
│       └── mock_responses/        # Saved API responses for testing
│
└── scripts/                       # Temporary test/utility scripts
    ├── test_pagination.py
    ├── test_entity_access.py
    ├── generate_mocks.py
    └── verify_filtering.py
```

## Component Design

### 1. Authentication Manager (lib/auth.py)

**Responsibilities:**
- Discover tenant ID from WWW-Authenticate header
- Obtain OAuth token via client credentials flow
- Auto-refresh token with 3000s (50 min) expiry window
- Thread-safe token storage

**Interface:**
```python
class DataverseAuth:
    def get_token(self) -> str
    def _discover_tenant_id(self) -> str
    def _refresh_token(self)
```

### 2. Configuration Loader (lib/config.py)

**Responsibilities:**
- Load credentials from .env file
- Load and validate entities_config.json
- Provide typed configuration objects

**Data structures:**
```python
@dataclass
class Config:
    api_url, client_id, client_secret, scope, sqlite_db_path

@dataclass
class EntityConfig:
    name: str              # API plural name
    filtered: bool         # True for accounts/contacts
    description: str
```

### 3. Dataverse API Client (lib/dataverse_client.py)

**Responsibilities:**
- Async HTTP with aiohttp and semaphore (50 concurrent max)
- Exponential backoff retry: [1, 2, 4, 8, 16] seconds
- Handle 429 (rate limit with Retry-After), 401 (token refresh), timeouts, 5xx
- Pagination via @odata.nextLink with proper headers

**Critical headers:**
```python
{
    'Authorization': f'Bearer {token}',
    'Accept': 'application/json',
    'OData-MaxVersion': '4.0',
    'OData-Version': '4.0',
    'Prefer': 'odata.maxpagesize=5000'  # Enable pagination
}
```

**Key methods:**
```python
async def fetch_with_retry(url: str, attempt: int = 0) -> Dict
async def fetch_all_pages(entity_name: str, orderby: str,
                          filter_query: Optional[str] = None) -> List[Dict]
```

### 4. Schema Manager (lib/schema_manager.py)

**Responsibilities:**
- Discover schema from first record (GET entity?$top=1)
- Detect primary key with fallback algorithm
- Infer SQLite types from sample values
- Compare schemas and detect changes (added/removed/type-changed columns)
- Persist schema to schema.json

**Primary key detection algorithm:**
1. Try `{entity_name}id` (e.g., vin_candidatesid)
2. Try singular form: remove 's', add 'id' (e.g., vin_candidateid) ✓
3. Find shortest column ending with 'id' not starting with '_'
4. Fallback to first column

**Type inference:**
```python
None → TEXT
int → INTEGER
float → REAL
bool → INTEGER
str (ISO datetime) → TEXT
str (GUID) → TEXT
dict/list → TEXT
```

**Schema comparison:**
- Breaking changes: removed entities, removed columns, type changes → alert but continue
- Info changes: new entities, new columns → log and continue (stored in json_response)

### 5. Database Manager (lib/database.py)

**Responsibilities:**
- Create tables with schema plus special columns
- UPSERT records (INSERT OR REPLACE)
- Create indexes for modifiedon/createdon
- Manage sync metadata tables (_sync_state, _sync_log)

**Note on Airflow deployment:**
Storing sync state in the target database (rather than external files) makes the script stateless and suitable for Airflow orchestration. The target DB becomes the single source of truth for both data and sync metadata.

**Table structure:**
```sql
CREATE TABLE vin_candidates (
  -- Regular columns from schema
  vin_candidateid TEXT PRIMARY KEY,
  vin_name TEXT,
  modifiedon TEXT,
  -- ... all discovered columns ...

  -- Special sync columns
  json_response TEXT,    -- Complete API response with @odata metadata
  sync_time TEXT,        -- When we synced (UTC ISO)
  valid_from TEXT        -- Dataverse's modifiedon timestamp
);

CREATE INDEX idx_vin_candidates_modifiedon ON vin_candidates(modifiedon);
```

**UPSERT logic:**
```python
def upsert_records(entity_name, schema, records) -> (added, updated):
    for record in records:
        # Check if exists
        # Count as added or updated
        # Store: regular columns + json_response + sync_time + valid_from
        # INSERT OR REPLACE
```

### 6. Sync State Tracker (lib/sync_state.py)

**Responsibilities:**
- Track sync progress in _sync_state table
- Enable resumability after failures
- Log sync history in _sync_log table

**State transitions:**
```
pending → in_progress → completed
                     ↘ failed
```

## Sync Implementation Flow

### Phase 1: Initialization
1. Load config from .env and entities_config.json
2. Authenticate and obtain token
3. Initialize database and sync tables
4. Load or discover schema

### Phase 2: Schema Management

**First sync:**
1. For each entity, fetch sample record
2. Detect primary key and infer column types
3. Save schema.json
4. Create tables with regular + special columns

**Subsequent syncs:**
1. Load schema.json
2. Fetch sample records from live API
3. Compare schemas
4. Alert on changes (breaking or info)
5. Continue with existing schema (no ALTER TABLE)

### Phase 3: Sync Unfiltered Entities (Parallel)

For each unfiltered entity:
1. Get last sync timestamp from _sync_state
2. Build incremental filter if has modifiedon:
   ```
   modifiedon gt {last_timestamp}
   ```
3. Determine orderby (pk → createdon → modifiedon → none)
4. Fetch all pages with pagination
5. UPSERT records
6. Update sync state

Run all unfiltered entities in parallel with semaphore limiting.

### Phase 4: Extract Linked IDs

After unfiltered sync completes:

**For accounts:**
```python
# Query junction tables
SELECT DISTINCT accountid FROM vin_vin_candidate_accountset
UNION
SELECT DISTINCT accountid FROM vin_vin_clinicaltrial_accountset
UNION
SELECT DISTINCT accountid FROM vin_vin_clinicaltrial_account_collaboratorset
```

**For contacts:**
```python
# Query foreign key columns (_*_value fields)
# e.g., _createdby_value, _modifiedby_value, etc.
```

### Phase 5: Sync Filtered Entities

For accounts and contacts:
1. Batch linked IDs (50 per request to avoid URL length limits)
2. Build $filter query:
   ```
   accountid eq 'id1' or accountid eq 'id2' or ...
   ```
3. Fetch with pagination
4. UPSERT records
5. Update sync state

### Phase 6: Completion

1. Generate summary report
2. Log sync statistics
3. Update _sync_state for all entities

## Idempotency Strategy

### Incremental Sync via modifiedon

```python
last_timestamp = get_last_sync_timestamp(entity_name)

if last_timestamp and schema.has_modifiedon:
    # Fetch only changed records
    filter_query = f"modifiedon gt {last_timestamp}"
else:
    # Full sync (first run or no modifiedon)
    filter_query = None

# Fetch with filter
records = fetch_all_pages(entity_name, orderby, filter_query)

# Find max timestamp in results
max_timestamp = max(r['modifiedon'] for r in records if 'modifiedon' in r)

# Store for next sync
update_last_sync_timestamp(entity_name, max_timestamp)
```

### Entities Without modifiedon

Junction tables typically don't have modifiedon - require full sync each time (acceptable as they're small).

## Testing Strategy

### Temporary Test Scripts (scripts/)

Before implementation, create diagnostic scripts:

1. **test_pagination.py**: Discover optimal page sizes
2. **test_entity_access.py**: Verify all entities accessible
3. **generate_mocks.py**: Fetch real data, save as mock responses
4. **verify_filtering.py**: Test $filter queries

Delete after verification or keep as diagnostic tools.

### Mock API Responses

Store in tests/fixtures/mock_responses/:
- vin_candidate_page1.json (5000 records)
- vin_candidate_page2.json (remaining records)
- accounts_filtered.json
- error_responses/ (429, 401, 400, etc.)

### Unit Tests

Test each utility module independently:
- test_auth.py: Token lifecycle, expiry, refresh
- test_client.py: Retry logic, pagination, semaphore
- test_schema.py: PK detection, type inference, comparison
- test_database.py: Table creation, UPSERT, JSON storage
- test_sync_state.py: State transitions

### Integration Tests

- test_integration_full_sync.py: Complete first sync flow with mocks
- test_integration_incremental_sync.py: Modify mocks, verify incremental sync

## Implementation Steps

### Step 1: Update entities_config.json ✓

Change entity names from singular to plural:
- vin_candidate → vin_candidates
- account → accounts
- contact → contacts
- etc.

### Step 2: Create Project Structure

```bash
mkdir -p clean/{lib,tests/{fixtures/mock_responses},scripts}
touch clean/{lib,tests}/__init__.py
```

### Step 3: Generate Mock Data

```bash
cd clean/scripts
python generate_mocks.py  # Fetch sample data for testing
```

### Step 4: Build Utility Modules (Bottom-Up with Tests)

Order matters - build dependencies first:

1. **Config module** → test_config.py → validate
2. **Auth module** → test_auth.py → validate
3. **Client module** → test_client.py → validate
4. **Schema module** → test_schema.py → validate
5. **Database module** → test_database.py → validate
6. **State module** → test_sync_state.py → validate

Run pytest after each module to verify before proceeding.

### Step 5: Integration Test Script

Create scripts/test_full_sync.py for manual integration testing with real API.

### Step 6: Main Sync Script

Implement clean/sync_dataverse.py with clear phases:
```python
async def main():
    print("[1/6] Loading configuration...")
    print("[2/6] Authenticating...")
    print("[3/6] Initializing database...")
    print("[4/6] Discovering/validating schema...")
    print("[5/6] Syncing data...")
    print("[6/6] Generating summary...")
```

### Step 7: Test and Validate

```bash
# Unit tests
pytest tests/ -v

# Integration tests (mocked)
pytest tests/test_integration*.py -v

# Real API test (small subset - 2-3 entities)
python sync_dataverse.py

# Full sync
python sync_dataverse.py

# Incremental sync test
python sync_dataverse.py  # Should fetch only new/modified
```

### Step 8: Clean Up

Review scripts/ directory:
- Keep diagnostic tools
- Remove temporary dev scripts
- Document remaining scripts in README

## Success Criteria

### Functional Requirements ✓
- First sync discovers schema, creates tables with json_response/sync_time/valid_from
- Subsequent syncs validate schema, alert on changes, continue with existing
- Incremental sync uses modifiedon filters
- Parallel execution (50 concurrent)
- Pagination with @odata.nextLink
- Retry with exponential backoff
- Auto token refresh
- Filtered entities via $filter queries
- Idempotent (no redownload)
- Resumable after failures

### Non-Functional Requirements ✓
- Testable: >80% coverage
- Reusable: lib/ modules importable
- Maintainable: clear separation of concerns
- Observable: detailed logging
- Safe: schema changes don't break syncs
- Documented: comprehensive docs

## Critical Files

Files to reference during implementation:

1. **clean/entities_config.json** - Update entity names to plurals
2. **sync_dataverse_to_sqlite.py** - Reference for proven patterns:
   - Pagination strategy with Prefer header + $orderby
   - Retry logic with exponential backoff
   - Token refresh implementation
   - Primary key detection algorithm
   - UPSERT logic
3. **schema.json** - Schema serialization format
4. **SYNC_COMPLETE.md** - Lessons learned and working patterns

## Dependencies

```txt
# Production
aiohttp>=3.9.0
requests>=2.31.0
python-dotenv>=1.0.0

# Testing
pytest>=7.4.0
pytest-asyncio>=0.21.0
pytest-mock>=3.11.0
```

## Key Lessons from Existing Implementation

1. **Must use `Prefer: odata.maxpagesize=5000` header** for pagination
2. **Must use `$orderby` parameter** for deterministic paging
3. **Cannot mix `$top` with `Prefer` header** - they conflict
4. **Junction tables may not support all ordering** - need fallbacks
5. **Primary key detection needs multiple strategies** - singular/plural handling
6. **Store complete JSON response** - enables schema evolution without data loss
7. **Incremental sync saves massive time** - only fetch changed records
8. **Parallel execution is crucial** - 99x faster than sequential
