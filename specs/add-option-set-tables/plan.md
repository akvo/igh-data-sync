# Option Set Detection and Handling - Plan C (Infer from Data)

## Why This Approach

After testing, the EntityDefinitions API approach is **technically possible but impractical**:

✅ **EntityDefinitions API works** with type-casted queries:
```
EntityDefinitions(LogicalName='<entity>')/Attributes(<metadata-id>)/Microsoft.Dynamics.CRM.PicklistAttributeMetadata?$expand=OptionSet
```

❌ **But requires too many API calls**:
- Production has **82 local option sets** per entity (tested with `vin_candidate`)
- Would need **83 API calls per entity** (1 base + 82 for option sets)
- Across **29 entities**: ~**2,400+ API calls** just for metadata
- Too slow and inefficient

✅ **Plan C (Infer from Data) is superior**:
- No upfront metadata fetching
- Simple implementation
- Self-correcting (automatically handles new option values)
- Works with any Dataverse instance
- Fast (no thousands of extra API calls)

## How Plan C Works

Dataverse API responses already include both code and label:
```json
{
  "statuscode": 1,
  "statuscode@OData.Community.Display.V1.FormattedValue": "Active",
  "categories": "1,2",
  "categories@OData.Community.Display.V1.FormattedValue": "Category A;Category B"
}
```

**Strategy**: Detect and build option sets dynamically during sync by:
1. Identifying INTEGER fields with `@FormattedValue` annotations
2. Creating lookup/junction tables on first encounter
3. Populating them as we sync data

## Implementation Plan

### Step 1: Create Option Set Detector

**File**: `lib/sync/optionset_detector.py` (NEW)

```python
"""Detect option sets from API response data."""

from dataclasses import dataclass
from typing import Optional


@dataclass
class DetectedOptionSet:
    """An option set detected from API response."""
    field_name: str
    is_multi_select: bool
    codes_and_labels: dict[int, str]  # {1: "Active", 2: "Inactive"}


class OptionSetDetector:
    """Detects option sets from API response records."""

    def detect_from_record(self, api_record: dict) -> dict[str, DetectedOptionSet]:
        """
        Detect option sets from a single API record.

        Args:
            api_record: API response record

        Returns:
            Dict mapping field name to DetectedOptionSet
        """
        detected = {}

        for key in api_record.keys():
            # Look for @FormattedValue annotations
            if key.endswith("@OData.Community.Display.V1.FormattedValue"):
                # Extract base field name
                field_name = key.replace("@OData.Community.Display.V1.FormattedValue", "")

                # Get raw value
                raw_value = api_record.get(field_name)
                formatted_value = api_record.get(key)

                if raw_value is None or formatted_value is None:
                    continue

                # Determine if multi-select
                is_multi_select = self._is_multi_select(raw_value, formatted_value)

                # Extract codes and labels
                codes_and_labels = self._extract_codes_and_labels(
                    raw_value, formatted_value, is_multi_select
                )

                if codes_and_labels:
                    detected[field_name] = DetectedOptionSet(
                        field_name=field_name,
                        is_multi_select=is_multi_select,
                        codes_and_labels=codes_and_labels,
                    )

        return detected

    def _is_multi_select(self, raw_value: any, formatted_value: str) -> bool:
        """
        Determine if this is a multi-select option set.

        Multi-select indicators:
        - Formatted value contains semicolons
        - Raw value is comma-separated string of codes
        """
        if isinstance(formatted_value, str) and ";" in formatted_value:
            return True
        if isinstance(raw_value, str) and "," in raw_value:
            return True
        return False

    def _extract_codes_and_labels(
        self, raw_value: any, formatted_value: str, is_multi_select: bool
    ) -> dict[int, str]:
        """
        Extract code-label mappings.

        Args:
            raw_value: Raw integer code(s) from API
            formatted_value: Formatted label(s) from API
            is_multi_select: Whether this is multi-select

        Returns:
            Dict mapping code to label
        """
        codes_and_labels = {}

        try:
            if is_multi_select:
                # Multi-select: Parse comma-separated codes and semicolon-separated labels
                if isinstance(raw_value, str):
                    codes = [int(c.strip()) for c in raw_value.split(",") if c.strip()]
                else:
                    # Sometimes multi-select raw values are already integers
                    codes = [int(raw_value)]

                labels = [
                    label.strip()
                    for label in formatted_value.split(";")
                    if label.strip()
                ]

                # Match codes to labels
                for code, label in zip(codes, labels):
                    codes_and_labels[code] = label

            else:
                # Single-select: Direct mapping
                code = int(raw_value)
                codes_and_labels[code] = formatted_value

        except (ValueError, TypeError):
            # Skip if we can't parse as integer
            pass

        return codes_and_labels
```

### Step 2: Update DatabaseManager to Create Tables Dynamically

**File**: `lib/sync/database.py`

Add methods after `init_sync_tables()`:

```python
def ensure_optionset_table(self, field_name: str) -> None:
    """
    Create option set lookup table if it doesn't exist.

    Args:
        field_name: Name of the option set field
    """
    table_name = f"_optionset_{field_name}"

    if self.table_exists(table_name):
        return

    # Create lookup table
    self.execute(f"""
        CREATE TABLE {table_name} (
            code INTEGER PRIMARY KEY,
            label TEXT NOT NULL,
            first_seen TEXT NOT NULL
        )
    """)

    print(f"  ✓ Created option set lookup table '{table_name}'")


def ensure_junction_table(
    self, entity_name: str, field_name: str, entity_pk: str
) -> None:
    """
    Create junction table for multi-select option set if it doesn't exist.

    Args:
        entity_name: Name of the entity table
        field_name: Name of the multi-select field
        entity_pk: Primary key column of entity table
    """
    table_name = f"_junction_{entity_name}_{field_name}"

    if self.table_exists(table_name):
        return

    lookup_table = f"_optionset_{field_name}"

    # Create junction table with foreign keys
    self.execute(f"""
        CREATE TABLE {table_name} (
            entity_id TEXT NOT NULL,
            option_code INTEGER NOT NULL,
            PRIMARY KEY (entity_id, option_code),
            FOREIGN KEY (entity_id) REFERENCES {entity_name}({entity_pk}),
            FOREIGN KEY (option_code) REFERENCES {lookup_table}(code)
        )
    """)

    print(f"  ✓ Created junction table '{table_name}'")


def upsert_option_set_value(self, field_name: str, code: int, label: str) -> None:
    """
    Insert or update an option set value in the lookup table.

    Args:
        field_name: Name of the option set field
        code: Option code (integer value)
        label: Display label for the option
    """
    if not self.conn:
        self.connect()

    # Ensure table exists
    self.ensure_optionset_table(field_name)

    table_name = f"_optionset_{field_name}"
    cursor = self.conn.cursor()

    # Check if code exists
    cursor.execute(f"SELECT label FROM {table_name} WHERE code = ?", (code,))
    existing = cursor.fetchone()

    if existing:
        # Update label if changed (keep original first_seen)
        if existing[0] != label:
            cursor.execute(
                f"UPDATE {table_name} SET label = ? WHERE code = ?",
                (label, code),
            )
    else:
        # Insert new option
        from datetime import datetime, timezone
        first_seen = datetime.now(timezone.utc).isoformat()
        cursor.execute(
            f"INSERT INTO {table_name} (code, label, first_seen) VALUES (?, ?, ?)",
            (code, label, first_seen),
        )

    self.conn.commit()


def populate_detected_option_sets(
    self,
    detected: dict,  # Dict[str, DetectedOptionSet]
    api_record: dict,
    entity_name: str,
    entity_id: str,
    entity_pk: str,
) -> None:
    """
    Populate option set data from detected option sets.

    Args:
        detected: Dict of detected option sets from OptionSetDetector
        api_record: API response record
        entity_name: Name of the entity
        entity_id: Primary key value of the entity
        entity_pk: Primary key column name
    """
    for field_name, option_set in detected.items():
        if option_set.is_multi_select:
            # Multi-select: Populate lookup and junction tables
            self.ensure_optionset_table(field_name)
            self.ensure_junction_table(entity_name, field_name, entity_pk)

            # Clear old junction records
            self.clear_junction_records(entity_name, field_name, entity_id)

            # Populate
            for code, label in option_set.codes_and_labels.items():
                self.upsert_option_set_value(field_name, code, label)
                self.upsert_junction_record(entity_name, field_name, entity_id, code)

        else:
            # Single-select: Just populate lookup table
            self.ensure_optionset_table(field_name)

            for code, label in option_set.codes_and_labels.items():
                self.upsert_option_set_value(field_name, code, label)


def upsert_junction_record(
    self, entity_name: str, field_name: str, entity_id: str, option_code: int
) -> None:
    """
    Insert junction record for multi-select option set.

    Args:
        entity_name: Name of the entity table
        field_name: Name of the multi-select field
        entity_id: ID of the entity record
        option_code: Option code to link
    """
    if not self.conn:
        self.connect()

    table_name = f"_junction_{entity_name}_{field_name}"
    cursor = self.conn.cursor()

    # INSERT OR IGNORE (duplicate prevention)
    cursor.execute(
        f"INSERT OR IGNORE INTO {table_name} (entity_id, option_code) VALUES (?, ?)",
        (entity_id, option_code),
    )

    self.conn.commit()


def clear_junction_records(
    self, entity_name: str, field_name: str, entity_id: str
) -> None:
    """
    Clear all junction records for an entity before re-inserting.

    Args:
        entity_name: Name of the entity table
        field_name: Name of the multi-select field
        entity_id: ID of the entity record
    """
    if not self.conn:
        self.connect()

    table_name = f"_junction_{entity_name}_{field_name}"

    # Check if table exists first
    if not self.table_exists(table_name):
        return

    cursor = self.conn.cursor()
    cursor.execute(f"DELETE FROM {table_name} WHERE entity_id = ?", (entity_id,))
    self.conn.commit()
```

### Step 3: Integrate into Sync Process

**File**: `lib/sync/database.py` - Update `upsert_batch()` method

Find the `upsert_batch` method and update it to detect and populate option sets:

```python
def upsert_batch(
    self,
    table_name: str,
    primary_key: str,
    schema: TableSchema,
    api_records: list[dict],
) -> tuple[int, int]:
    """
    Batch upsert records with option set detection and storage.

    Args:
        table_name: Table name
        primary_key: Primary key column name
        schema: TableSchema for column mapping
        api_records: List of API response records

    Returns:
        Tuple of (records_added, records_updated)
    """
    # Import detector
    from .optionset_detector import OptionSetDetector

    detector = OptionSetDetector()
    added = 0
    updated = 0

    for api_record in api_records:
        # Get entity_id from api_record (primary key value)
        entity_id = api_record.get(primary_key)

        if not entity_id:
            continue

        # STEP 1: Detect option sets from this record
        detected_option_sets = detector.detect_from_record(api_record)

        # STEP 2: Populate option set data (lookup and junction tables)
        if detected_option_sets:
            self.populate_detected_option_sets(
                detected_option_sets,
                api_record,
                table_name,
                entity_id,
                primary_key,
            )

        # STEP 3: Map columns from schema for entity table
        record = {}
        for col in schema.columns:
            if col.name in api_record:
                # Store raw value (INTEGER for option sets)
                value = api_record[col.name]

                # Handle multi-select: don't store in entity table
                # (will be in junction table instead)
                field_name = col.name
                if field_name in detected_option_sets:
                    option_set = detected_option_sets[field_name]
                    if option_set.is_multi_select:
                        # Skip - multi-select stored in junction table only
                        continue

                record[col.name] = value

        # Add special columns
        record["json_response"] = json.dumps(api_record)
        record["sync_time"] = datetime.now(timezone.utc).isoformat()
        record["valid_from"] = api_record.get("modifiedon")

        # STEP 4: Upsert entity record
        is_new = self.upsert(table_name, primary_key, record)
        if is_new:
            added += 1
        else:
            updated += 1

    return added, updated
```

### Step 4: Testing

**File**: `tests/unit/sync/test_optionset_detector.py` (NEW)

```python
"""Tests for option set detector."""

import pytest

from lib.sync.optionset_detector import OptionSetDetector


class TestOptionSetDetector:
    """Test option set detection from API records."""

    def test_detect_single_select(self):
        """Test detecting single-select option set."""
        detector = OptionSetDetector()

        api_record = {
            "accountid": "acc123",
            "statuscode": 1,
            "statuscode@OData.Community.Display.V1.FormattedValue": "Active",
        }

        detected = detector.detect_from_record(api_record)

        assert "statuscode" in detected
        option_set = detected["statuscode"]
        assert option_set.field_name == "statuscode"
        assert option_set.is_multi_select is False
        assert option_set.codes_and_labels == {1: "Active"}

    def test_detect_multi_select(self):
        """Test detecting multi-select option set."""
        detector = OptionSetDetector()

        api_record = {
            "accountid": "acc123",
            "categories": "1,2",
            "categories@OData.Community.Display.V1.FormattedValue": "Category A;Category B",
        }

        detected = detector.detect_from_record(api_record)

        assert "categories" in detected
        option_set = detected["categories"]
        assert option_set.field_name == "categories"
        assert option_set.is_multi_select is True
        assert option_set.codes_and_labels == {1: "Category A", 2: "Category B"}

    def test_detect_multiple_option_sets(self):
        """Test detecting multiple option sets in one record."""
        detector = OptionSetDetector()

        api_record = {
            "accountid": "acc123",
            "statuscode": 1,
            "statuscode@OData.Community.Display.V1.FormattedValue": "Active",
            "categories": "1,2",
            "categories@OData.Community.Display.V1.FormattedValue": "Category A;Category B",
        }

        detected = detector.detect_from_record(api_record)

        assert len(detected) == 2
        assert "statuscode" in detected
        assert "categories" in detected

    def test_ignore_non_integer_codes(self):
        """Test ignoring fields with non-integer codes."""
        detector = OptionSetDetector()

        api_record = {
            "name": "Test Account",
            "name@OData.Community.Display.V1.FormattedValue": "Test Account",
        }

        detected = detector.detect_from_record(api_record)

        # Should not detect 'name' as option set (not an integer)
        assert "name" not in detected

    def test_missing_formatted_value(self):
        """Test handling missing formatted value."""
        detector = OptionSetDetector()

        api_record = {
            "accountid": "acc123",
            "statuscode": 1,
            # No @FormattedValue annotation
        }

        detected = detector.detect_from_record(api_record)

        # Should not detect statuscode without formatted value
        assert "statuscode" not in detected
```

**File**: `tests/unit/sync/test_database_optionset_detection.py` (NEW)

```python
"""Tests for database option set detection integration."""

import pytest

from lib.sync.database import DatabaseManager
from lib.type_mapping import TableSchema


class TestDatabaseOptionSetDetection:
    """Test option set detection in database operations."""

    @pytest.fixture(autouse=True)
    def setup(self, tmp_path):
        """Create temp database."""
        db_path = tmp_path / "test.db"
        self.db = DatabaseManager(str(db_path))
        self.db.connect()
        yield
        self.db.close()

    def test_ensure_optionset_table(self):
        """Test creating option set lookup table."""
        self.db.ensure_optionset_table("statuscode")

        assert self.db.table_exists("_optionset_statuscode") is True

        # Calling again should be idempotent
        self.db.ensure_optionset_table("statuscode")
        assert self.db.table_exists("_optionset_statuscode") is True

    def test_ensure_junction_table(self):
        """Test creating junction table."""
        # First create entity and lookup tables
        self.db.execute("CREATE TABLE accounts (accountid TEXT PRIMARY KEY)")
        self.db.ensure_optionset_table("categories")

        # Create junction table
        self.db.ensure_junction_table("accounts", "categories", "accountid")

        assert self.db.table_exists("_junction_accounts_categories") is True

    def test_upsert_option_set_value_creates_table(self):
        """Test that upsert creates table if needed."""
        self.db.upsert_option_set_value("statuscode", 1, "Active")

        # Table should exist
        assert self.db.table_exists("_optionset_statuscode") is True

        # Value should be inserted
        cursor = self.db.conn.cursor()
        cursor.execute("SELECT code, label FROM _optionset_statuscode WHERE code = 1")
        row = cursor.fetchone()

        assert row is not None
        assert row[0] == 1
        assert row[1] == "Active"

    def test_populate_detected_option_sets_single_select(self):
        """Test populating single-select option set."""
        from lib.sync.optionset_detector import DetectedOptionSet

        # Create entity table
        self.db.execute("CREATE TABLE accounts (accountid TEXT PRIMARY KEY, statuscode INTEGER)")

        detected = {
            "statuscode": DetectedOptionSet(
                field_name="statuscode",
                is_multi_select=False,
                codes_and_labels={1: "Active", 2: "Inactive"},
            )
        }

        api_record = {
            "accountid": "acc123",
            "statuscode": 1,
        }

        self.db.populate_detected_option_sets(
            detected, api_record, "accounts", "acc123", "accountid"
        )

        # Check lookup table populated
        cursor = self.db.conn.cursor()
        cursor.execute("SELECT code, label FROM _optionset_statuscode ORDER BY code")
        rows = cursor.fetchall()

        assert len(rows) == 2
        assert rows[0][0] == 1
        assert rows[0][1] == "Active"
        assert rows[1][0] == 2
        assert rows[1][1] == "Inactive"

    def test_populate_detected_option_sets_multi_select(self):
        """Test populating multi-select option set."""
        from lib.sync.optionset_detector import DetectedOptionSet

        # Create entity table
        self.db.execute("CREATE TABLE accounts (accountid TEXT PRIMARY KEY)")

        detected = {
            "categories": DetectedOptionSet(
                field_name="categories",
                is_multi_select=True,
                codes_and_labels={1: "Category A", 2: "Category B"},
            )
        }

        api_record = {
            "accountid": "acc123",
            "categories": "1,2",
        }

        self.db.populate_detected_option_sets(
            detected, api_record, "accounts", "acc123", "accountid"
        )

        # Check lookup table populated
        cursor = self.db.conn.cursor()
        cursor.execute("SELECT code, label FROM _optionset_categories ORDER BY code")
        rows = cursor.fetchall()

        assert len(rows) == 2
        assert rows[0][0] == 1
        assert rows[0][1] == "Category A"

        # Check junction table populated
        cursor.execute(
            "SELECT entity_id, option_code FROM _junction_accounts_categories ORDER BY option_code"
        )
        junction_rows = cursor.fetchall()

        assert len(junction_rows) == 2
        assert junction_rows[0][0] == "acc123"
        assert junction_rows[0][1] == 1
```

### Step 5: Manual Verification

After implementation:

```bash
# Delete DB and run fresh sync
rm -f dataverse_complete.db
source .venv/bin/activate && python sync_dataverse.py

# Check option set tables created
sqlite3 dataverse_complete.db <<EOF
SELECT name FROM sqlite_master
WHERE type='table' AND (name LIKE '_optionset%' OR name LIKE '_junction%')
ORDER BY name;
EOF

# Check a lookup table has data
sqlite3 dataverse_complete.db <<EOF
.mode column
.headers on
SELECT * FROM _optionset_statuscode LIMIT 10;
EOF

# Verify joins work
sqlite3 dataverse_complete.db <<EOF
SELECT c.vin_candidateid, c.statuscode, s.label
FROM vin_candidates c
LEFT JOIN _optionset_statuscode s ON c.statuscode = s.code
LIMIT 5;
EOF
```

## Benefits of Plan C

1. ✅ **Simple**: No complex metadata API queries
2. ✅ **Fast**: No thousands of extra API calls
3. ✅ **Reliable**: Works with any Dataverse instance
4. ✅ **Self-correcting**: Automatically detects new option values
5. ✅ **Battle-tested pattern**: Inferring from data is common in ETL
6. ✅ **Already proven**: API responses DO include both code and label

## Files to Create/Modify

**New Files**:
- `lib/sync/optionset_detector.py` - Option set detection logic
- `tests/unit/sync/test_optionset_detector.py` - Detector tests
- `tests/unit/sync/test_database_optionset_detection.py` - Integration tests

**Modified Files**:
- `lib/sync/database.py` - Add dynamic table creation and detection integration

**No Changes Needed**:
- `lib/validation/metadata_parser.py` - Not involved
- `lib/type_mapping.py` - Not needed for Plan C
- `lib/sync/schema_initializer.py` - Not needed (tables created dynamically)
