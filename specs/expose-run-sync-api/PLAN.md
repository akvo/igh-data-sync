# Expose Sync Command as Callable Async Function

## Overview

Refactor `src/igh_data_sync/scripts/sync.py` to expose a clean async function that can be called programmatically from Apache Airflow DAGs or other Python scripts. The function will be exported from the package root for easy import.

## User Requirements

- Function signature: `async def run_sync(config: Config, entities_config=None, optionsets_config=None, verify_reference=False)`
- Export from package root: `from igh_data_sync import run_sync`
- Handle authentication internally (user just passes Config with credentials)
- Return simple boolean: True on success, False on failure
- Silent by default with optional logging parameter

## Current State Analysis

### Existing Function Structure

```
main() [CLI entry point]
  └── asyncio.run(async_main())
      ├── _load_configuration() → (Config, list[EntityConfig])
      ├── _authenticate() → access_token
      └── async with DataverseClient + DatabaseManager:
          └── run_sync_workflow() → None (prints to console)
```

### Key Findings

1. **`run_sync_workflow()` is already well-designed** for testability:
   - Takes client, config, entities, db_manager, verify_references
   - Contains core sync logic without CLI concerns
   - **Issue**: Returns None, uses print(), doesn't aggregate results

2. **Authentication happens in `async_main()`**:
   - `DataverseAuth(config).get_token()` is synchronous
   - Token passed to `DataverseClient(config, token)` async context manager

3. **Results are calculated but not returned**:
   - `_sync_unfiltered_entities()` returns (total_added, total_updated, failed_entities)
   - `_sync_filtered_entities()` returns (total_added, total_updated)
   - Stats printed but not returned to caller

4. **Exit handling via `sys.exit()`**:
   - Validation errors: `sys.exit(1)` in `validate_schema_before_sync()`
   - Reference errors: `sys.exit(1)` in `_verify_references_if_needed()`
   - Should be replaced with return values for programmatic use

5. **`optionsets_config` parameter is unused**:
   - Accepted by `async_main()` but never passed to any function
   - Currently option sets are auto-detected during sync
   - Can be included in signature for future use or removed

## Implementation Plan

### Step 1: Modify `run_sync_workflow()` to Return Results

**File**: `src/igh_data_sync/scripts/sync.py`

**Changes**:
1. Update `run_sync_workflow()` to return a dict with results:
   ```python
   return {
       "success": bool,
       "total_added": int,
       "total_updated": int,
       "failed_entities": list[tuple[str, str]],  # [(entity_name, error_msg), ...]
       "validation_errors": list[dict],  # If validation fails
       "reference_errors": list[str],  # If verify_references=True
   }
   ```

2. Capture validation results from `validate_schema_before_sync()`:
   - Currently returns `(valid_entities, entities_to_create, differences)`
   - Check if any `differences` have `severity == "error"`
   - If errors exist, return early with `success=False` instead of calling `sys.exit(1)`

3. Capture reference verification results from `_verify_references_if_needed()`:
   - Currently calls `sys.exit(1)` if dangling references found
   - Instead, collect errors and return them in results dict

4. Aggregate all results from helper functions:
   - Combine `total_added` and `total_updated` from unfiltered and filtered syncs
   - Collect all `failed_entities` from both sync phases
   - Set `success = (len(failed_entities) == 0 and len(validation_errors) == 0)`

### Step 2: Add Optional Logging Support

**File**: `src/igh_data_sync/scripts/sync.py`

**Changes**:
1. Add `logger: Optional[logging.Logger] = None` parameter to:
   - `run_sync_workflow()`
   - All helper functions (`_sync_unfiltered_entities`, `_sync_filtered_entities`, etc.)

2. Replace all `print()` statements:
   ```python
   # Before
   print(f"  Syncing {len(unfiltered)} unfiltered entities...")

   # After
   if logger:
       logger.info(f"Syncing {len(unfiltered)} unfiltered entities...")
   ```

3. Keep print statements that are currently in the codebase, but wrap them:
   ```python
   def _log_or_print(message: str, logger: Optional[logging.Logger] = None):
       if logger:
           logger.info(message)
       # Don't print anything if no logger provided (silent mode)
   ```

### Step 3: Create Public `run_sync()` Function

**File**: `src/igh_data_sync/scripts/sync.py`

**Add new function** (place after `run_sync_workflow`, before `async_main`):

```python
async def run_sync(
    config: Config,
    entities_config: Optional[list[EntityConfig]] = None,
    optionsets_config: Optional[dict] = None,
    verify_reference: bool = False,
    logger: Optional[logging.Logger] = None,
) -> bool:
    """
    Programmatic async entry point for Dataverse sync.

    Designed for use from Apache Airflow or other orchestration tools.
    Handles authentication, database connection, and complete sync workflow.

    Args:
        config: Configuration object with Dataverse API credentials and database settings.
                Required fields: api_url, client_id, client_secret, scope, sqlite_db_path
        entities_config: List of EntityConfig objects to sync.
                        If None, loads from default package data (entities_config.json)
        optionsets_config: Dict mapping entity names to option set field lists.
                          If None, loads from default package data (optionsets.json).
                          Currently unused by core logic but included for future use.
        verify_reference: If True, verify foreign key reference integrity after sync.
                         Sync will fail (return False) if dangling references found.
        logger: Optional Python logger for output. If None, runs silently.

    Returns:
        bool: True if sync completed successfully, False if any errors occurred
              (validation failures, sync errors, or reference integrity issues)

    Raises:
        ValueError: If config is missing required fields
        RuntimeError: If authentication fails

    Example:
        ```python
        from igh_data_sync import run_sync
        from igh_data_sync.config import Config
        import logging

        config = Config(
            api_url="https://org.api.crm.dynamics.com/api/data/v9.2/",
            client_id="...",
            client_secret="...",
            scope="...",
            sqlite_db_path="dataverse.db"
        )

        logger = logging.getLogger(__name__)
        success = await run_sync(config, verify_reference=True, logger=logger)

        if not success:
            raise Exception("Sync failed")
        ```
    """
    # [1] Load entities config if not provided
    if entities_config is None:
        if logger:
            logger.info("Loading entities from package defaults")
        entities_config = load_entities_config()  # Uses package data

    # [2] Authenticate with Dataverse
    if logger:
        logger.info("Authenticating with Dataverse")
    try:
        auth = DataverseAuth(config)
        token = auth.get_token()
    except Exception as e:
        if logger:
            logger.error(f"Authentication failed: {e}")
        raise RuntimeError(f"Authentication failed: {e}") from e

    # [3] Run sync workflow with context managers
    if logger:
        logger.info("Starting sync workflow")

    try:
        async with DataverseClient(config, token) as client:
            with DatabaseManager(config.sqlite_db_path) as db_manager:
                results = await run_sync_workflow(
                    client=client,
                    config=config,
                    entities=entities_config,
                    db_manager=db_manager,
                    verify_references=verify_reference,
                    logger=logger,
                )

        # [4] Determine success from results
        success = results["success"]

        if logger:
            if success:
                logger.info(
                    f"Sync completed successfully: "
                    f"+{results['total_added']} added, "
                    f"{results['total_updated']} updated"
                )
            else:
                logger.error(
                    f"Sync failed: "
                    f"{len(results['failed_entities'])} entity failures, "
                    f"{len(results.get('validation_errors', []))} validation errors"
                )

        return success

    except Exception as e:
        if logger:
            logger.exception("Sync workflow failed with exception")
        raise
```

### Step 4: Update CLI `async_main()` to Use New Function

**File**: `src/igh_data_sync/scripts/sync.py`

**Refactor `async_main()`** to call `run_sync()`:

```python
async def async_main(verify_references=False, env_file=None, entities_config=None, optionsets_config=None):
    """
    CLI entry point - wraps run_sync() with CLI-specific concerns.

    Handles:
    - Loading configuration from files/environment
    - Setting up console output (print statements)
    - Exit codes based on success/failure
    """
    print("=" * 60)
    print("DATAVERSE TO SQLITE SYNC")
    print("=" * 60)

    try:
        # [1] Load configuration
        print("\n[1/2] Loading configuration...")
        config = load_config(env_file=env_file)

        # Load entities if custom path provided
        entities = None
        if entities_config:
            entities = load_entities_config(path=entities_config)

        print("✓ Configuration loaded")

        # [2] Run sync (with console output via print)
        print("\n[2/2] Running sync workflow...")

        # Create a simple logger that prints to console
        import logging
        console_logger = logging.getLogger("sync")
        console_logger.setLevel(logging.INFO)
        handler = logging.StreamHandler()
        handler.setFormatter(logging.Formatter("%(message)s"))
        console_logger.addHandler(handler)

        success = await run_sync(
            config=config,
            entities_config=entities,
            optionsets_config=optionsets_config,
            verify_reference=verify_references,
            logger=console_logger,
        )

        if success:
            print("\n✓ Sync completed successfully")
            sys.exit(0)
        else:
            print("\n❌ Sync failed - check logs for details")
            sys.exit(1)

    except KeyboardInterrupt:
        print("\n\nInterrupted by user")
        sys.exit(1)
    except Exception as e:
        print(f"\n❌ SYNC FAILED: {e}")
        traceback.print_exc()
        sys.exit(1)
```

**Note**: The above keeps CLI print statements but delegates actual sync work to `run_sync()`.

### Step 5: Export `run_sync` from Package Root

**File**: `src/igh_data_sync/__init__.py`

**Add import and export**:

```python
"""Microsoft Dataverse integration toolkit with SCD2 temporal tracking."""

from igh_data_sync.scripts.sync import run_sync

__version__ = "0.1.0"

__all__ = ["__version__", "run_sync"]
```

This allows users to import as: `from igh_data_sync import run_sync`

### Step 6: Update `load_entities_config()` Function Name

**File**: `src/igh_data_sync/config.py`

**Rename function** for consistency with `load_config()`:

```python
# Current: load_entities(path: Optional[str] = None) -> list[str]
# New:     load_entities_config(path: Optional[str] = None) -> list[EntityConfig]
```

**Update all usages**:
- `src/igh_data_sync/scripts/sync.py`
- `src/igh_data_sync/scripts/validate.py`
- `src/igh_data_sync/scripts/optionset.py`
- `tests/` (if any tests use this function)

**Also update return type**: Currently returns `list[str]` (entity names), should return `list[EntityConfig]` objects for programmatic use.

### Step 7: Handle `optionsets_config` Parameter

**Decision**: Include parameter in signature but document as unused for now.

**Reasoning**:
- User's requested signature includes it
- Future-proofing: may be needed for schema initialization
- Currently option sets are auto-detected during sync
- Can be utilized later without breaking API

**Documentation**: Add note in docstring:
```
optionsets_config: Dict mapping entity names to option set field lists.
                  If None, loads from default package data (optionsets.json).
                  Note: Currently unused by core sync logic (option sets are
                  auto-detected). Included for future enhancement.
```

## Critical Files to Modify

1. **src/igh_data_sync/scripts/sync.py** (primary changes)
   - Modify `run_sync_workflow()` to return results dict
   - Add optional logger parameter to all functions
   - Create new `run_sync()` public function
   - Refactor `async_main()` to call `run_sync()`
   - Update all helper functions to support logging

2. **src/igh_data_sync/__init__.py** (export)
   - Add `from igh_data_sync.scripts.sync import run_sync`
   - Add to `__all__`

3. **src/igh_data_sync/config.py** (minor refactor)
   - Rename `load_entities()` → `load_entities_config()`
   - Update return type to `list[EntityConfig]`

4. **src/igh_data_sync/validation/validator.py** (return changes)
   - Modify `validate_schema_before_sync()` to return errors instead of calling `sys.exit(1)`

5. **src/igh_data_sync/sync/reference_verifier.py** (return changes)
   - Modify verification to return errors instead of calling `sys.exit(1)`

## Verification Steps

### 1. Unit Tests

Create `tests/unit/test_run_sync.py`:

```python
import pytest
from unittest.mock import Mock, AsyncMock
from igh_data_sync import run_sync
from igh_data_sync.config import Config, EntityConfig

@pytest.mark.asyncio
async def test_run_sync_success(mocker, test_config):
    """Test run_sync returns True on successful sync."""
    # Mock authentication
    mocker.patch("igh_data_sync.scripts.sync.DataverseAuth")

    # Mock run_sync_workflow to return success
    mock_workflow = mocker.patch("igh_data_sync.scripts.sync.run_sync_workflow")
    mock_workflow.return_value = {
        "success": True,
        "total_added": 10,
        "total_updated": 5,
        "failed_entities": [],
        "validation_errors": [],
    }

    # Call run_sync
    success = await run_sync(test_config)

    assert success is True
    mock_workflow.assert_called_once()

@pytest.mark.asyncio
async def test_run_sync_failure(mocker, test_config):
    """Test run_sync returns False on sync failure."""
    mocker.patch("igh_data_sync.scripts.sync.DataverseAuth")

    mock_workflow = mocker.patch("igh_data_sync.scripts.sync.run_sync_workflow")
    mock_workflow.return_value = {
        "success": False,
        "total_added": 0,
        "total_updated": 0,
        "failed_entities": [("accounts", "API error")],
        "validation_errors": [],
    }

    success = await run_sync(test_config)

    assert success is False

@pytest.mark.asyncio
async def test_run_sync_with_logger(mocker, test_config):
    """Test run_sync uses logger when provided."""
    import logging

    mocker.patch("igh_data_sync.scripts.sync.DataverseAuth")
    mock_workflow = mocker.patch("igh_data_sync.scripts.sync.run_sync_workflow")
    mock_workflow.return_value = {"success": True, "total_added": 10, "total_updated": 5, "failed_entities": []}

    logger = Mock(spec=logging.Logger)

    await run_sync(test_config, logger=logger)

    # Verify logger was called
    assert logger.info.call_count > 0

@pytest.mark.asyncio
async def test_run_sync_auth_failure(mocker, test_config):
    """Test run_sync raises RuntimeError on auth failure."""
    mock_auth = mocker.patch("igh_data_sync.scripts.sync.DataverseAuth")
    mock_auth.return_value.get_token.side_effect = Exception("Auth failed")

    with pytest.raises(RuntimeError, match="Authentication failed"):
        await run_sync(test_config)

@pytest.mark.asyncio
async def test_run_sync_with_custom_entities(mocker, test_config):
    """Test run_sync accepts custom entities config."""
    mocker.patch("igh_data_sync.scripts.sync.DataverseAuth")
    mock_workflow = mocker.patch("igh_data_sync.scripts.sync.run_sync_workflow")
    mock_workflow.return_value = {"success": True, "total_added": 0, "total_updated": 0, "failed_entities": []}

    custom_entities = [
        EntityConfig(name="account", api_name="accounts", filtered=False, description="Test")
    ]

    await run_sync(test_config, entities_config=custom_entities)

    # Verify custom entities were passed to workflow
    call_args = mock_workflow.call_args
    assert call_args.kwargs["entities"] == custom_entities
```

### 2. Integration Test (Airflow-like Usage)

Create `tests/e2e/test_airflow_usage.py`:

```python
"""Test run_sync usage pattern from Apache Airflow."""
import asyncio
import logging
from pathlib import Path
import pytest

from igh_data_sync import run_sync
from igh_data_sync.config import Config

@pytest.mark.asyncio
async def test_airflow_dag_usage(temp_db, mock_credentials):
    """Simulate calling run_sync from an Airflow DAG task."""

    # Setup logging (as Airflow would)
    logger = logging.getLogger("airflow.task")
    logger.setLevel(logging.INFO)

    # Create config (Airflow would get from connections/variables)
    config = Config(
        api_url=mock_credentials["api_url"],
        client_id=mock_credentials["client_id"],
        client_secret=mock_credentials["client_secret"],
        scope=mock_credentials["scope"],
        sqlite_db_path=temp_db,
    )

    # Call run_sync (this is what Airflow DAG would do)
    success = await run_sync(
        config=config,
        verify_reference=True,
        logger=logger,
    )

    # Airflow would check success and mark task as success/failure
    assert success is True

    # Verify database was updated
    assert Path(temp_db).exists()
```

### 3. Manual Testing

Test the new public API:

```python
# test_manual_usage.py
import asyncio
import logging
from igh_data_sync import run_sync
from igh_data_sync.config import Config

async def main():
    # Setup
    logging.basicConfig(level=logging.INFO)
    logger = logging.getLogger(__name__)

    config = Config(
        api_url="https://your-org.api.crm.dynamics.com/api/data/v9.2/",
        client_id="your-client-id",
        client_secret="your-client-secret",
        scope="your-scope",
        sqlite_db_path="test_sync.db",
    )

    # Run sync
    success = await run_sync(
        config=config,
        verify_reference=True,
        logger=logger,
    )

    print(f"Sync {'succeeded' if success else 'failed'}")
    return 0 if success else 1

if __name__ == "__main__":
    exit_code = asyncio.run(main())
    exit(exit_code)
```

Run: `python test_manual_usage.py`

### 4. CLI Still Works

Verify CLI commands still work after refactoring:

```bash
# Test sync command
uv run sync-dataverse --help
uv run sync-dataverse --verify

# Test other commands
uv run validate-schema --help
uv run generate-optionset-config --help
```

### 5. Pytest Suite

Run full test suite to ensure no regressions:

```bash
uv run pytest -v
uv run pytest --cov=igh_data_sync --cov-report=term-missing
```

Expected: All 121 tests pass, coverage remains ≥65%

## Success Criteria

- ✅ `run_sync()` function is callable from external Python scripts
- ✅ Function can be imported as: `from igh_data_sync import run_sync`
- ✅ Function handles authentication internally (user just passes Config)
- ✅ Function returns boolean: True on success, False on failure
- ✅ Function is silent by default, logs when logger provided
- ✅ Function works with custom entities_config or package defaults
- ✅ CLI commands (`sync-dataverse`) still work unchanged
- ✅ All existing tests pass (121 tests)
- ✅ New unit tests for `run_sync()` added and passing
- ✅ Integration test simulating Airflow usage passes
- ✅ Code coverage remains ≥65%

## Notes

- **Backward compatibility**: CLI interface unchanged, existing scripts continue to work
- **Optional logger**: Airflow users can pass Airflow's task logger for integrated logging
- **No breaking changes**: All existing functionality preserved
- **Clean separation**: CLI code (printing, exit codes) stays in `async_main()`, core logic in `run_sync()`
- **Testability**: `run_sync()` is fully testable without CLI concerns
