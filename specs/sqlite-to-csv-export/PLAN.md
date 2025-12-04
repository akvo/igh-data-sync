# Plan: SQLite to CSV Export with Transitive Closure Filtering

## Task Overview

Create a CSV export tool for the IGH Dataverse SQLite database with:
1. Export all tables to CSV files (one per table)
2. Exclude `json_response` column, include all other columns including metadata
3. Generate markdown documentation with FK relationships and Mermaid ERD
4. Optional: Random candidate sampling with bidirectional transitive closure
5. Output to flat directory structure

## User Requirements

### Core Features
- Export each table to CSV (flat directory: output/table_name.csv)
- Exclude: `json_response` column only
- Include: All business data, `sync_time`, `valid_from` metadata columns
- Generate: `schema_relationships.md` with comprehensive Mermaid ERD (all relationships)

### Optional Filtering
- CLI parameter: `--sample-size N` (e.g., 100)
- Algorithm:
  1. Randomly select N candidates
  2. Follow FK references FROM candidates (forward: candidates → diseases, products, etc.)
  3. Find records referencing candidates (reverse: junction tables → candidates)
  4. Repeat until convergence (bidirectional transitive closure)

### Location
- Script: `./lib/csv_export/` (new module)
- Entry point: `./export_to_csv.py`
- Spec: `./specs/sqlite-to-csv-export/`

## Architecture

### Module Structure

```
lib/csv_export/
├── __init__.py                    # Package init
├── database_inspector.py          # DB schema introspection via PRAGMA
├── csv_exporter.py                # CSV export with batching
├── transitive_closure.py          # Bidirectional transitive closure
├── relationship_documenter.py     # Markdown + Mermaid ERD generation
└── cli.py                         # Argument parsing & orchestration
```

### Key Classes

**DatabaseInspector** (`database_inspector.py`)
- `get_all_tables() -> list[str]` - Query sqlite_master (exclude _sync_*)
- `get_table_columns(table) -> list[str]` - PRAGMA table_info
- `get_foreign_keys(table) -> list[ForeignKeyMetadata]` - PRAGMA foreign_key_list
- `get_primary_key(table) -> str` - Parse PRAGMA table_info
- `count_records(table) -> int` - SELECT COUNT(*)

**CSVExporter** (`csv_exporter.py`)
- `export_table(table, path, filter_ids=None) -> int` - Export with optional filtering
- `export_all_tables(output_dir, filtered_tables=None) -> dict` - Batch export with stats
- Uses standard library `csv` module, UTF-8-sig encoding (Excel compatible)
- Batches IN clauses (500 IDs) and fetchmany (1000 rows) for memory efficiency

**TransitiveClosureExtractor** (`transitive_closure.py`)
- `extract_from_candidates(candidate_ids, max_iterations=10) -> dict[str, set[str]]`
- `_get_forward_references(table, ids) -> dict[str, set[str]]` - Follow FKs FROM table
- `_get_reverse_references(table, ids) -> dict[str, set[str]]` - Find records referencing table
- Iterative convergence: stops when no new IDs found
- Returns: {table_name: set of IDs to export}

**RelationshipDocumenter** (`relationship_documenter.py`)
- `generate_markdown(output_path)` - Complete documentation
- `generate_mermaid_erd() -> str` - ERD with all relationships (comprehensive)
- Includes: table statistics, FK listing, per-table details

**CLI** (`cli.py`)
- Argparse interface with --database, --output, --sample-size, --seed, --verbose
- Workflow orchestration: validate → document → filter → export

## Implementation Details

### 1. CSV Export Strategy

**Column Filtering:**
```python
# Exclude ONLY json_response, include sync_time and valid_from
export_columns = [col for col in all_columns if col != 'json_response']
```

**CSV Writing:**
```python
# Use csv.DictWriter with UTF-8-sig (Excel BOM)
with open(path, 'w', encoding='utf-8-sig', newline='') as f:
    writer = csv.DictWriter(f, fieldnames=export_columns)
    writer.writeheader()

    # Stream results in batches
    while True:
        rows = cursor.fetchmany(1000)
        if not rows:
            break
        for row in rows:
            # NULL → empty string for CSV standard
            record = {col: row[col] or '' for col in export_columns}
            writer.writerow(record)
```

**Filtered Export with Batching:**
```python
# Batch IN clauses to avoid SQLite parameter limits
for batch in chunks(filter_ids, 500):
    placeholders = ','.join(['?'] * len(batch))
    sql = f"SELECT {cols} FROM {table} WHERE {pk_col} IN ({placeholders})"
    cursor = db.execute(sql, tuple(batch))
    # ... write rows
```

### 2. Transitive Closure Algorithm

**Bidirectional Approach:**

```python
def extract_from_candidates(candidate_ids, max_iterations=10):
    """Discover all related records via bidirectional traversal."""
    current_ids = {table: set() for table in all_tables}
    current_ids['vin_candidates'] = candidate_ids.copy()

    for iteration in range(1, max_iterations + 1):
        previous_total = sum(len(ids) for ids in current_ids.values())

        # Forward: Follow FKs FROM tables with IDs
        for table, ids in list(current_ids.items()):
            if ids:
                # e.g., candidates → diseases via _diseaseid_value
                forward = _get_forward_references(table, ids)
                for target_table, target_ids in forward.items():
                    current_ids[target_table].update(target_ids)

        # Reverse: Find records that reference tables with IDs
        for table, ids in list(current_ids.items()):
            if ids:
                # e.g., junction tables → candidates via candidateid
                reverse = _get_reverse_references(table, ids)
                for source_table, source_ids in reverse.items():
                    current_ids[source_table].update(source_ids)

        # Check convergence
        new_total = sum(len(ids) for ids in current_ids.values())
        if new_total == previous_total:
            print(f"Converged after {iteration} iterations")
            break

    return {t: ids for t, ids in current_ids.items() if ids}
```

**FK Detection via PRAGMA:**
```python
def get_foreign_keys(table_name):
    """Extract FKs using PRAGMA foreign_key_list."""
    cursor.execute(f"PRAGMA foreign_key_list({table_name})")
    fks = []
    for row in cursor.fetchall():
        # (id, seq, table, from, to, on_update, on_delete, match)
        fks.append(ForeignKeyMetadata(
            column=row[3],              # from
            referenced_table=row[2],    # table
            referenced_column=row[4]    # to
        ))
    return fks
```

### 3. Documentation Generation

**Markdown Structure:**
```markdown
# Database Schema Relationships

## Overview
- Total Tables: 28
- Entity Tables: 26
- Total Foreign Keys: 145

## Tables by Category
### Unfiltered Entities (22)
- vin_candidates (9,256 records)
- vin_products (X records)
...

### Filtered Entities (4)
- accounts (X records)
...

## Foreign Key Relationships
| Source Table | FK Column | References | Referenced Column |
|--------------|-----------|------------|-------------------|
| vin_candidates | _diseaseid_value | vin_diseases | vin_diseaseid |
...

## Entity Relationship Diagram
```mermaid
erDiagram
    vin_candidates }o--|| vin_diseases : "_diseaseid_value"
    vin_candidates }o--|| vin_products : "_productid_value"
    vin_vin_candidate_accountset }o--|| vin_candidates : "candidateid"
    vin_vin_candidate_accountset }o--|| accounts : "accountid"
    [... all relationships ...]
```

## Table Details
### vin_candidates
- **Primary Key**: vin_candidateid
- **Record Count**: 9,256
- **Column Count**: 235
- **Foreign Keys**:
  - _diseaseid_value → vin_diseases.vin_diseaseid
  ...
```

**Mermaid Generation:**
```python
def generate_mermaid_erd():
    """Generate comprehensive ERD with all relationships."""
    lines = ["erDiagram"]

    # Get all FK relationships
    for table in get_all_tables():
        fks = get_foreign_keys(table)
        for fk in fks:
            # Many-to-one cardinality
            lines.append(f'    {table} }}o--|| {fk.referenced_table} : "{fk.column}"')

    return "\n".join(lines)
```

### 4. CLI Interface

**Command Examples:**
```bash
# Export all tables
python export_to_csv.py --database dataverse_complete.db --output ./exports

# Export 100 random candidates with transitive closure
python export_to_csv.py --database dataverse_complete.db --output ./exports --sample-size 100

# Reproducible sampling with seed
python export_to_csv.py --database dataverse_complete.db --output ./exports --sample-size 100 --seed 42
```

**Arguments:**
- `--database PATH` (required) - SQLite database file
- `--output PATH` (required) - Output directory (flat structure)
- `--sample-size N` (optional) - Random candidate sample size
- `--max-iterations N` (default: 10) - Transitive closure limit
- `--seed N` (optional) - Random seed for reproducibility
- `--verbose` (flag) - Verbose logging
- `--docs-only` (flag) - Generate documentation only

**Output Structure (Flat):**
```
exports/
├── schema_relationships.md      # Documentation with ERD
├── vin_candidates.csv
├── vin_products.csv
├── vin_diseases.csv
├── accounts.csv
├── contacts.csv
└── ... (one CSV per table)
```

## Implementation Steps

### Phase 1: Core Infrastructure (2-3 hours)
1. Create `lib/csv_export/` module structure
2. Implement `DatabaseInspector` with PRAGMA-based methods
3. Write unit tests for `DatabaseInspector`
4. Verify FK detection on actual database

### Phase 2: CSV Export (3-4 hours)
1. Implement `CSVExporter.export_table()` with column exclusion
2. Implement batched queries for filtered export (500 ID chunks)
3. Test full and filtered export with actual database
4. Implement `export_all_tables()` with progress reporting

### Phase 3: Transitive Closure (4-5 hours)
1. Implement random candidate selection with seed support
2. Implement `_get_forward_references()` - follow FKs from table
3. Implement `_get_reverse_references()` - find records referencing table
4. Implement convergence loop in `extract_from_candidates()`
5. Write comprehensive unit tests with test database

### Phase 4: Documentation Generation (2-3 hours)
1. Implement markdown structure with statistics
2. Implement comprehensive Mermaid ERD generation
3. Test documentation output with actual database
4. Verify Mermaid syntax renders correctly

### Phase 5: CLI Integration (2-3 hours)
1. Implement argparse with all parameters
2. Implement workflow orchestration in `main()`
3. Create entry point script `export_to_csv.py` at repo root
4. End-to-end manual testing

### Phase 6: Testing & Documentation (2-3 hours)
1. Write comprehensive unit tests (`tests/test_csv_export.py`)
2. Create test fixtures with FK relationships
3. Write specification document in `specs/sqlite-to-csv-export/`
4. Add docstrings to all classes and methods
5. Final validation and testing

**Total Estimated Time: 15-21 hours**

## Key Design Decisions

### Decision: FK Detection Method
**Choice:** PRAGMA foreign_key_list (SQLite introspection)
**Rationale:** Direct access to actual DB constraints, no dependency on external metadata

### Decision: Bidirectional Traversal
**Choice:** Both forward (follow FKs) and reverse (find referencing records)
**Rationale:** Per user requirements - captures both referenced entities AND junction table relationships

### Decision: Column Exclusion
**Choice:** Exclude ONLY `json_response`, include `sync_time` and `valid_from`
**Rationale:** User preference - metadata columns useful for auditing

### Decision: Output Structure
**Choice:** Flat directory (all CSVs in one folder)
**Rationale:** User preference - simplicity over organization

### Decision: ERD Scope
**Choice:** Comprehensive (all relationships)
**Rationale:** User preference - complete documentation over simplified view

### Decision: CSV Library
**Choice:** Standard library `csv` module with UTF-8-sig encoding
**Rationale:** No dependencies, correct escaping, Excel compatibility (BOM)

### Decision: Batching Strategy
**Choice:** 500 IDs per IN clause, 1000 rows per fetchmany
**Rationale:** SQLite parameter limits (~32k max, 500 practical), memory efficiency

### Decision: Error Handling
**Choice:** Continue on error, report failures at end
**Rationale:** Maximize useful output, consistent with codebase pattern

## Testing Strategy

### Test Database Fixture
Create SQLite DB with:
- 3 diseases
- 5 candidates (with FK to diseases)
- 3 accounts
- Junction table linking candidates ↔ accounts

### Test Coverage
- **DatabaseInspector:** 100% (table/column/FK discovery)
- **CSVExporter:** 95%+ (export logic, batching, filtering)
- **TransitiveClosureExtractor:** 90%+ (forward/reverse, convergence)
- **RelationshipDocumenter:** 85%+ (markdown/Mermaid generation)
- **CLI:** 70%+ (via integration tests)

### Key Test Cases
1. Full export (all tables, all records)
2. Filtered export (specific IDs only)
3. Column exclusion (verify json_response excluded)
4. Transitive closure convergence
5. Random sampling reproducibility (with seed)
6. Bidirectional relationship discovery
7. FK detection via PRAGMA
8. Mermaid ERD generation

## Critical Files

**Implementation Files:**
1. `lib/csv_export/transitive_closure.py` - Most complex logic
2. `lib/csv_export/csv_exporter.py` - Core export functionality
3. `lib/csv_export/database_inspector.py` - Foundation for all operations
4. `lib/csv_export/cli.py` - User-facing interface & orchestration
5. `lib/csv_export/relationship_documenter.py` - Documentation generation

**Test Files:**
1. `tests/test_csv_export.py` - Comprehensive test suite

**Entry Point:**
1. `export_to_csv.py` - CLI entry point at repo root

**Dependencies (Existing):**
- `lib/sync/database.py` - DatabaseManager class
- `lib/type_mapping.py` - ForeignKeyMetadata dataclass

## Success Criteria

- ✅ Export all 26+ entity tables to CSV (flat directory)
- ✅ Exclude `json_response` column, include all other columns
- ✅ Generate comprehensive documentation with Mermaid ERD
- ✅ Support random candidate sampling (--sample-size N)
- ✅ Bidirectional transitive closure with convergence detection
- ✅ UTF-8-sig encoding (Excel compatible)
- ✅ Reproducible sampling (--seed parameter)
- ✅ 85%+ test coverage
- ✅ Clear CLI with --help documentation
- ✅ Error handling with summary reporting
