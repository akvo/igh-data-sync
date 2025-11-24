# Dataverse Schema Validator

A schema validation tool that detects differences between Microsoft Dataverse entity schemas and local database schemas by comparing against the authoritative $metadata XML.

## Features

- ✅ Fetches complete OData $metadata XML from Dataverse (~7 MB, 800+ entities)
- ✅ Parses entity schemas with columns, primary keys, and foreign keys
- ✅ Supports both SQLite and PostgreSQL databases
- ✅ Detects missing tables, columns, and type mismatches
- ✅ Generates JSON and Markdown validation reports
- ✅ Returns exit code 0 (passed) or 1 (failed) for CI/CD integration

## Architecture

The validator follows a clean 6-step workflow:

1. **Load Configuration** - Read credentials and entity list
2. **Authenticate** - OAuth authentication with automatic tenant discovery
3. **Fetch Dataverse Schemas** - Parse $metadata XML for authoritative schemas
4. **Query Database Schemas** - Extract schemas from SQLite/PostgreSQL
5. **Compare Schemas** - Detect differences (errors, warnings, info)
6. **Generate Reports** - Create JSON, Markdown reports and console summary

## Project Structure

```
igh-clean-sync/
├── validate_schema.py          # Main entrypoint
├── entities_config.json         # Entity configuration (23 entities)
├── .env                         # Credentials (not committed)
├── .env.example                 # Template
├── requirements.txt             # Dependencies
├── lib/                         # Reusable utilities
│   ├── auth.py                  # OAuth authentication
│   ├── dataverse_client.py      # Async HTTP client
│   ├── config.py                # Configuration loading
│   ├── type_mapping.py          # Data structures and type mappings
│   └── validation/              # Validation components
│       ├── metadata_parser.py   # Parse $metadata XML (KEY COMPONENT)
│       ├── dataverse_schema.py  # Fetch schemas from Dataverse
│       ├── database_schema.py   # Query database schemas
│       ├── schema_comparer.py   # Compare and detect differences
│       └── report_generator.py  # Generate reports
└── tests/                       # Unit tests (31 tests, all passing)
    ├── test_type_mapping.py
    ├── test_metadata_parser.py
    └── test_schema_comparer.py
```

## Installation

```bash
# Install dependencies
pip install -r requirements.txt

# Copy environment template
cp .env.example .env

# Edit .env with your credentials
nano .env
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

Lists entities to validate:
```json
{
  "entities": [
    {
      "name": "vin_candidate",
      "filtered": false,
      "description": "All candidate records"
    },
    ...
  ]
}
```

## Usage

### Basic validation (auto-detect database type):
```bash
python3 validate_schema.py
```

### Specify database type:
```bash
python3 validate_schema.py --db-type sqlite
python3 validate_schema.py --db-type postgresql
```

### Custom report paths:
```bash
python3 validate_schema.py \
  --json-report reports/schema.json \
  --md-report reports/schema.md
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

Run the test suite (31 tests):
```bash
python3 -m unittest discover tests/ -v
```

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

- ✅ Fetches $metadata XML (~7 MB, 800+ entities)
- ✅ Extracts all requested entity schemas
- ✅ Detects missing tables in database
- ✅ Detects column differences (missing, extra, type mismatches)
- ✅ Detects primary key differences
- ✅ Detects foreign key differences
- ✅ Generates JSON and Markdown reports
- ✅ Exit code 0 = passed, 1 = failed
- ✅ 31 unit tests, all passing

## License

[Add your license here]
