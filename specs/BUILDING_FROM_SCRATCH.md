# Building the Schema Validator from Scratch

A guide to implementing a Dataverse schema validator that detects schema changes and ensures database completeness.

## Goals

1. **Detect schema changes** - Identify when Dataverse entity schemas change
2. **Ensure completeness** - Verify local database is a complete structural copy of Dataverse

## Core Architecture Principles

### Single Responsibility
Each component does one thing:
- **lib/auth.py** - OAuth authentication only
- **lib/dataverse_client.py** - HTTP requests only
- **lib/config.py** - Configuration loading only
- **lib/type_mapping.py** - Data structures and type conversions only
- **lib/validation/** - Validation components (fetch, query, compare, report)

### Clear Data Flow
```
.env → Config → Auth → Token → Client → Schemas → Comparison → Reports
```

### Reusable Utilities
Components in `lib/` are reusable by other scripts (data sync, exports, etc.)

## Critical Design Decision: Use $metadata XML

**Why $metadata XML, not EntityDefinitions API or sample data inference?**

### Problems with alternatives:
- **EntityDefinitions API**: Doesn't work for all entities (returns 400 errors)
- **Sample data inference**: Circular logic (infer from data → validate against data = always matches)

### Why $metadata XML wins:
- ✅ **Authoritative** - Official OData CSDL schema
- ✅ **Complete** - ALL columns, even if null in data
- ✅ **Type-accurate** - Exact Edm types (Int32 vs Decimal, String vs Memo)
- ✅ **Non-circular** - Schema from metadata, validation against actual database
- ✅ **Includes relationships** - Foreign keys via NavigationProperty elements

## Implementation Steps

### 1. Create Reusable Utilities (lib/)

**lib/config.py**
```python
@dataclass
class Config:
    api_url: str
    client_id: str
    client_secret: str
    scope: str
    sqlite_db_path: str

def load_config() -> Config:
    # Load from .env using python-dotenv

def load_entities(path: str) -> List[str]:
    # Load entity names from entities_config.json
```

**lib/auth.py**
```python
class DataverseAuth:
    def authenticate(self) -> str:
        # 1. Discover tenant ID from WWW-Authenticate header
        # 2. Request token from Microsoft identity platform
        # 3. Return access token
```

**lib/dataverse_client.py**
```python
class DataverseClient:
    async def get(self, endpoint: str, params: Optional[Dict] = None):
        # CRITICAL: Detect $metadata endpoint and use Accept: application/xml
        # For other endpoints use Accept: application/json
        # Return XML as text, JSON as dict
```

**lib/type_mapping.py**
```python
# Define dataclasses:
# - ColumnMetadata (name, types, nullable, max_length)
# - ForeignKeyMetadata (column, referenced_table, referenced_column)
# - IndexMetadata (name, columns, is_unique)
# - TableSchema (entity_name, columns, primary_key, foreign_keys, indexes)
# - SchemaDifference (entity, issue_type, severity, description, details)

# Edm type mappings:
EDM_TYPE_MAP_SQLITE = {
    'Edm.String': 'TEXT',
    'Edm.Int32': 'INTEGER',
    'Edm.Decimal': 'REAL',
    'Edm.Guid': 'TEXT',
    # ... etc
}
```

### 2. Create Validation Components (lib/validation/)

**lib/validation/metadata_parser.py** - THE CRITICAL COMPONENT
```python
class MetadataParser:
    def parse_metadata_xml(self, xml_content: str) -> Dict[str, TableSchema]:
        # 1. Parse XML using xml.etree.ElementTree
        # 2. Find all EntityType elements (namespace: http://docs.oasis-open.org/odata/ns/edm)
        # 3. Skip Abstract="true" entities
        # 4. For each entity:
        #    - Extract Name attribute
        #    - Parse Key/PropertyRef → primary_key
        #    - Parse all Property elements → columns (with Type, Nullable, MaxLength)
        #    - Parse NavigationProperty/ReferentialConstraint → foreign_keys
        # 5. Map Edm.X types to database types
        # 6. Return Dict[entity_name, TableSchema]
```

**Key XML structure to parse:**
```xml
<EntityType Name="vin_candidate">
  <Key><PropertyRef Name="vin_candidateid"/></Key>
  <Property Name="vin_name" Type="Edm.String" MaxLength="100" Nullable="true"/>
  <Property Name="vin_statuscode" Type="Edm.Int32"/>
  <NavigationProperty Name="createdby" Type="mscrm.systemuser">
    <ReferentialConstraint Property="_createdby_value" ReferencedProperty="systemuserid"/>
  </NavigationProperty>
</EntityType>
```

**lib/validation/dataverse_schema.py**
```python
class DataverseSchemaFetcher:
    async def fetch_schemas_from_metadata(self, entity_names: List[str]) -> Dict[str, TableSchema]:
        # 1. Fetch $metadata XML via client.get('$metadata')
        # 2. Parse using MetadataParser
        # 3. Filter to requested entity_names
        # 4. Return schemas
```

**lib/validation/database_schema.py**
```python
class DatabaseSchemaQuery:
    def query_all_schemas(self, entity_names: List[str]) -> Dict[str, TableSchema]:
        # SQLite: Use PRAGMA table_info, foreign_key_list, index_list
        # PostgreSQL: Query information_schema tables
        # Return TableSchema objects
```

**lib/validation/schema_comparer.py**
```python
class SchemaComparer:
    def compare_all(self, dataverse_schemas, database_schemas) -> List[SchemaDifference]:
        # 1. Missing tables/entities
        # 2. Column differences (missing, extra, type mismatch)
        # 3. Primary key differences
        # 4. Foreign key differences
        # Return list of SchemaDifference with severity (error/warning/info)
```

**lib/validation/report_generator.py**
```python
class ReportGenerator:
    def generate_json_report(self, differences, schemas, ...):
        # Output schema_validation_report.json

    def generate_markdown_report(self, differences, schemas, ...):
        # Output schema_validation_report.md with human-readable format

    def print_summary(self, differences, schemas):
        # Print summary to console, return bool (passed/failed)
```

### 3. Create Main Entrypoint (validate_schema.py)

**Clear 6-step workflow:**
```python
async def main():
    # [1/6] Load Configuration
    config = load_config()
    entities = load_entities('entities_config.json')

    # [2/6] Authenticate with Dataverse
    auth = DataverseAuth(config)
    token = auth.authenticate()

    # [3/6] Fetch Dataverse Schemas
    async with DataverseClient(config, token) as client:
        fetcher = DataverseSchemaFetcher(client, target_db='sqlite')
        dataverse_schemas = await fetcher.fetch_schemas_from_metadata(entities)

    # [4/6] Query Database Schemas
    db_query = DatabaseSchemaQuery(config, db_type='sqlite')
    database_schemas = db_query.query_all_schemas(entities)

    # [5/6] Compare Schemas
    comparer = SchemaComparer(target_db='sqlite')
    differences = comparer.compare_all(dataverse_schemas, database_schemas)

    # [6/6] Generate Reports
    reporter = ReportGenerator()
    reporter.generate_json_report(differences, ...)
    reporter.generate_markdown_report(differences, ...)
    passed = reporter.print_summary(differences, ...)

    sys.exit(0 if passed else 1)
```

## Critical Implementation Details

### Entity Names: Use Logical Names (Singular)
- ❌ Wrong: `vin_candidates`, `accounts` (EntitySetName - plural)
- ✅ Correct: `vin_candidate`, `account` (LogicalName - singular)
- This is what appears in $metadata XML `<EntityType Name="vin_candidate">`

### Accept Headers for $metadata
```python
if '$metadata' in endpoint:
    headers = {'Accept': 'application/xml'}  # CRITICAL!
else:
    headers = {'Accept': 'application/json'}
```

### Edm Type → Database Type Mapping
```python
# SQLite
'Edm.String' → 'TEXT'
'Edm.Int32' → 'INTEGER'
'Edm.Decimal' → 'REAL'
'Edm.Boolean' → 'INTEGER'
'Edm.DateTimeOffset' → 'TEXT'
'Edm.Guid' → 'TEXT'

# PostgreSQL
'Edm.String' → 'VARCHAR(n)' or 'TEXT'
'Edm.Int32' → 'INTEGER'
'Edm.Decimal' → 'NUMERIC'
'Edm.Boolean' → 'BOOLEAN'
'Edm.DateTimeOffset' → 'TIMESTAMP WITH TIME ZONE'
'Edm.Guid' → 'UUID'
```

### Foreign Key Detection
NavigationProperty elements with ReferentialConstraint indicate FK relationships:
```xml
<NavigationProperty Name="createdby" Type="mscrm.systemuser">
  <ReferentialConstraint Property="_createdby_value" ReferencedProperty="systemuserid"/>
</NavigationProperty>
```
- FK column: `_createdby_value`
- References: `systemuser.systemuserid`

## Testing the Implementation

### 1. Manually Test End-to-End
```bash
python validate_schema.py --db-type sqlite
# Should:
# - Fetch $metadata (7+ MB XML)
# - Parse 800+ entities
# - Extract requested entities
# - Compare with database
# - Generate reports
```

### 2. Automated Tests for Schema Parser
```python
# - Parse saved XML
# - Verify correct number of entities extracted
# - Check specific entities have columns, PKs, FKs
```

### 3. Automated Tests for Report Generation/Comparison to existing DB 
```python
# - Use Saved Schema Parser Output 
# - Compare with database
# - Generate reports - correctly detects differences
```

## Dependencies

```txt
aiohttp>=3.9.0      # Async HTTP client
python-dotenv>=1.0.0 # .env file loading
requests>=2.31.0     # Sync HTTP for auth
```

## Example File Structure

```
clean/
├── validate_schema.py          # Main entrypoint (~180 lines)
├── entities_config.json         # Entity configuration
├── .env                         # Credentials (not committed)
├── .env.example                 # Template
├── requirements.txt             # Dependencies
├── lib/                         # Reusable utilities
│   ├── __init__.py
│   ├── auth.py                  # ~80 lines
│   ├── dataverse_client.py      # ~100 lines
│   ├── config.py                # ~60 lines
│   ├── type_mapping.py          # ~170 lines
│   └── validation/              # Validation components
│       ├── __init__.py
│       ├── metadata_parser.py   # ~300 lines - THE KEY COMPONENT
│       ├── dataverse_schema.py  # ~230 lines
│       ├── database_schema.py   # ~150 lines
│       ├── schema_comparer.py   # ~200 lines
│       └── report_generator.py  # ~250 lines
├── tests/                         # Reusable utilities
│   ├── ... #Tests following the lib file structure based on the component used as the test entrypoint (for unit tests and integration tests) 
└── README.md
```

## Success Criteria

✅ Validator fetches $metadata XML (~7 MB, 800+ entities)
✅ Extracts all requested entity schemas
✅ Detects missing tables in database
✅ Detects column differences (missing, extra, type mismatches)
✅ Detects primary key differences
✅ Detects foreign key differences
✅ Generates JSON and Markdown reports
✅ Exit code 0 = passed, 1 = failed - detected differences

## Why This Architecture Works

1. **Non-circular validation** - Schema source (Dataverse $metadata) is independent of validation target (database)
2. **Complete detection** - Catches missing columns that would be null in sample data
3. **Type-accurate** - Distinguishes Int32 vs Decimal, String vs Memo
4. **Single source of truth** - $metadata is authoritative OData schema
5. **Reusable components** - Auth, client, config can be used by data sync scripts
6. **Clear separation** - Fetch, query, compare, report are independent stages
7. **Testable** - Each component can be unit tested independently
