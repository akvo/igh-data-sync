# Package Restructuring Implementation Plan

## Overview

Convert the current script-based Dataverse integration toolkit into a proper distributable Python package following modern packaging standards with src/ layout and Hatchling build system.

**User Requirements:**
- Package name: `igh-data-sync` (import as `igh_data_sync`)
- CLI commands installed via setuptools entry points
- Config files packaged with CLI parameter overrides
- Environment variable loading: CLI param → .env in working dir → system env vars
- No backward compatibility with root-level config files

## Current Implementation

**Structure:**
- Script-based toolkit: `python sync_dataverse.py`, `python validate_schema.py`, etc.
- Library code in `lib/` directory
- Config files at repository root: `entities_config.json`, `config/optionsets.json`
- Dependencies in `requirements.txt` and `requirements-dev.txt`
- Package metadata in `pyproject.toml` but no build system

**Entry Points:**
- `sync_dataverse.py` - Main sync script (341 lines)
- `validate_schema.py` - Schema validation script (131 lines)
- `generate_optionset_config.py` - Config generation utility (172 lines)

## Package Structure Transformation

### Current Structure
```
igh-data-sync/
├── lib/                          # Core library
│   ├── auth.py
│   ├── config.py
│   ├── dataverse_client.py
│   ├── type_mapping.py
│   ├── sync/
│   └── validation/
├── tests/                        # Tests
├── sync_dataverse.py            # Script
├── validate_schema.py           # Script
├── generate_optionset_config.py # Script
├── entities_config.json         # Config
├── config/optionsets.json       # Config
├── requirements.txt             # Dependencies
├── requirements-dev.txt         # Dev dependencies
└── pyproject.toml               # Partial config
```

### Target Structure
```
igh-data-sync/
├── src/
│   └── igh_data_sync/           # Main package
│       ├── __init__.py          # Package init with version
│       ├── scripts/             # CLI entry points
│       │   ├── __init__.py
│       │   ├── sync.py          # sync-dataverse command
│       │   ├── validate.py      # validate-schema command
│       │   └── optionset.py     # generate-optionset-config command
│       ├── data/                # Package data (default configs)
│       │   ├── entities_config.json
│       │   └── optionsets.json
│       ├── auth.py
│       ├── config.py
│       ├── dataverse_client.py
│       ├── type_mapping.py
│       ├── sync/
│       │   └── ... (all existing modules)
│       └── validation/
│           └── ... (all existing modules)
├── tests/                       # Tests (imports from igh_data_sync)
├── .python-version              # Python version (3.9)
├── .gitignore                   # Updated with build artifacts
├── pyproject.toml               # Complete project config
└── README.md                    # Updated with installation instructions
```

## Implementation Steps

### 1. Create Package Structure

**Create src/igh_data_sync/ directory:**
- Create `src/` directory
- Create `src/igh_data_sync/` package directory
- Move all files from `lib/` to `src/igh_data_sync/`
- Create `src/igh_data_sync/scripts/` directory for CLI entry points
- Create `src/igh_data_sync/data/` directory for default configs

**Files to create:**
- `src/igh_data_sync/__init__.py` - Package version and metadata
- `src/igh_data_sync/scripts/__init__.py` - Empty init
- `src/igh_data_sync/scripts/sync.py` - Move content from `sync_dataverse.py`
- `src/igh_data_sync/scripts/validate.py` - Move content from `validate_schema.py`
- `src/igh_data_sync/scripts/optionset.py` - Move content from `generate_optionset_config.py`

### 2. Update pyproject.toml

**Add build system section:**
```toml
[build-system]
requires = ["hatchling"]
build-backend = "hatchling.build"
```

**Update project metadata:**
```toml
[project]
name = "igh-data-sync"
version = "0.1.0"
description = "Microsoft Dataverse integration toolkit with SCD2 temporal tracking"
readme = "README.md"
requires-python = ">=3.9"
authors = [
    { name = "Akvo", email = "tech@akvo.org" }
]
```

**Add dependencies (migrate from requirements.txt):**
```toml
dependencies = [
    "aiohttp>=3.9.0",
    "python-dotenv>=1.0.0",
    "requests>=2.31.0",
]
```

**Add optional dev dependencies (migrate from requirements-dev.txt):**
```toml
[project.optional-dependencies]
dev = [
    "ruff>=0.1.13",
    "pylint>=3.0.0",
    "mypy>=1.8.0",
    "types-requests>=2.31.0",
    "pytest>=7.4.0",
    "pytest-cov>=4.1.0",
    "pytest-asyncio>=0.21.0",
    "pytest-mock>=3.15.0",
    "aioresponses>=0.7.8",
    "responses>=0.25.0",
    "pre-commit>=3.5.0",
]
```

**Add CLI entry points:**
```toml
[project.scripts]
sync-dataverse = "igh_data_sync.scripts.sync:main"
validate-schema = "igh_data_sync.scripts.validate:main"
generate-optionset-config = "igh_data_sync.scripts.optionset:main"
```

**Update coverage config:**
```toml
[tool.coverage.run]
source = ["src/igh_data_sync"]
omit = ["tests/*", "*/site-packages/*"]
```

**Update mypy config:**
```toml
[[tool.mypy.overrides]]
module = "igh_data_sync.*"
warn_return_any = true
disallow_untyped_defs = false
```

### 3. Create CLI Entry Point Scripts

**src/igh_data_sync/scripts/sync.py:**
- Move all logic from `sync_dataverse.py`
- Keep the `main()` function as entry point
- Update imports: `from lib.` → `from igh_data_sync.`
- Add CLI arguments:
  - `--entities-config PATH` - Override entities config file location
  - `--optionsets-config PATH` - Override optionsets config file location
  - `--env-file PATH` - Specify .env file location
  - `--verify` - Existing flag for reference integrity verification
- Environment variable precedence: CLI param → .env from working dir → system env vars

**src/igh_data_sync/scripts/validate.py:**
- Move all logic from `validate_schema.py`
- Keep the `main()` function as entry point
- Update imports: `from lib.` → `from igh_data_sync.`
- Add CLI arguments:
  - `--entities-config PATH` - Override entities config file location
  - `--env-file PATH` - Specify .env file location
  - `--db-type DBTYPE` - Existing argument (sqlite/postgres)
  - `--json-report PATH` - Existing argument
  - `--md-report PATH` - Existing argument

**src/igh_data_sync/scripts/optionset.py:**
- Move all logic from `generate_optionset_config.py`
- Keep the `main()` function as entry point
- Add CLI arguments:
  - `--env-file PATH` - Specify .env file location
  - `--entities-config PATH` - Override entities config file location
  - `--db PATH` - Existing argument for database path

### 4. Update Configuration Loading (config.py)

**Modify `src/igh_data_sync/config.py`:**
- Add function to get package data path using `importlib.resources`
- Update `load_entities_config()` to accept optional path parameter:
  1. If path provided via CLI argument, use it
  2. Otherwise, use package data `igh_data_sync/data/entities_config.json`
- Update `load_optionsets_config()` similarly
- Add new function `load_optionsets_config()`
- Update environment variable loading to accept optional .env file path:
  1. If .env path provided via CLI argument, load from that path
  2. Otherwise, check for `.env` in current working directory
  3. Otherwise, use system environment variables

**Example implementation:**
```python
from importlib.resources import files
from pathlib import Path
from typing import Optional

def get_default_config_path(filename: str) -> str:
    """Get default config file path from package data."""
    return str(files('igh_data_sync').joinpath(f'data/{filename}'))

def load_entities_config(path: Optional[str] = None) -> list[EntityConfig]:
    """Load entities config from path or package default."""
    if path is None:
        path = get_default_config_path('entities_config.json')
    # ... load from path

def load_config(env_file: Optional[str] = None) -> Config:
    """Load environment variables: CLI path → working dir .env → system vars."""
    if env_file:
        load_dotenv(env_file)
    elif Path('.env').exists():
        load_dotenv('.env')
    # Otherwise, use system environment variables
```

### 5. Update All Internal Imports

**Files to update (all Python files in src/igh_data_sync/):**
- Change `from lib.` → `from igh_data_sync.`
- Change `from lib.sync.` → `from igh_data_sync.sync.`
- Change `from lib.validation.` → `from igh_data_sync.validation.`

**Method:**
```bash
find src/igh_data_sync -name "*.py" -type f -exec sed -i 's/from lib\./from igh_data_sync./g' {} +
```

**Critical files:**
- `src/igh_data_sync/sync/*.py` (8 files)
- `src/igh_data_sync/validation/*.py` (6 files)
- `src/igh_data_sync/scripts/*.py` (3 files)
- `src/igh_data_sync/*.py` (4 core files)

### 6. Update Tests

**Update all test imports:**
- Change `from lib.` → `from igh_data_sync.`
- Update `tests/conftest.py` imports
- Update `tests/unit/` imports (all test files)
- Update `tests/e2e/` imports
- Update `tests/helpers/` imports

**Method:**
```bash
find tests -name "*.py" -type f -exec sed -i 's/from lib\./from igh_data_sync./g' {} +
```

**Python 3.9 Compatibility:**
- Replace `str | None` type hints with `Optional[str]`
- Add `from typing import Optional` imports where needed
- Files to update: config.py, scripts/*.py, sync/filtered_sync.py, validation/validator.py, tests/helpers/fake_dataverse_client.py

### 7. Add .python-version File

Create `.python-version` with content:
```
3.9
```

This ensures UV and pyenv use Python 3.9 for development.

### 8. Move Default Configs to Package Data

**Move configuration files:**
- `entities_config.json` → `src/igh_data_sync/data/entities_config.json`
- `config/optionsets.json` → `src/igh_data_sync/data/optionsets.json`

**Note:** Original files will be deleted in step 10. Users can override defaults using CLI parameters.

### 9. Update Documentation

**README.md updates:**
- Add installation section with pip and UV methods
- Update command examples: `python sync_dataverse.py` → `sync-dataverse`
- Add section on CLI parameters for config/env file overrides
- Document environment variable loading precedence
- Document config file loading: CLI parameter or package defaults

**CLAUDE.md updates:**
- Update module organization section with new paths (lib/ → src/igh_data_sync/)
- Update development commands to use installed CLI commands
- Add section on package structure and distribution
- Update import examples throughout
- Document CLI parameter usage
- Add migration guide from script-based to package structure

### 10. Clean Up Old Files

**After verifying the new structure works:**
- Delete `sync_dataverse.py` (moved to src/igh_data_sync/scripts/sync.py)
- Delete `validate_schema.py` (moved to src/igh_data_sync/scripts/validate.py)
- Delete `generate_optionset_config.py` (moved to src/igh_data_sync/scripts/optionset.py)
- Delete `lib/` directory (moved to src/igh_data_sync/)
- Delete `requirements.txt` (migrated to pyproject.toml)
- Delete `requirements-dev.txt` (migrated to pyproject.toml)
- Delete `entities_config.json` (moved to src/igh_data_sync/data/)
- Delete `config/` directory (optionsets.json moved to src/igh_data_sync/data/)

**Update .gitignore:**
- Verify `dist/`, `*.egg-info/`, `build/` are already present (they are)

## Critical Files to Modify

1. `pyproject.toml` - Add dependencies, entry points, build system
2. `src/igh_data_sync/__init__.py` - Package version and metadata
3. `src/igh_data_sync/config.py` - Update config loading logic
4. `src/igh_data_sync/scripts/sync.py` - Migrate from sync_dataverse.py
5. `src/igh_data_sync/scripts/validate.py` - Migrate from validate_schema.py
6. `src/igh_data_sync/scripts/optionset.py` - Migrate from generate_optionset_config.py
7. All Python files in `src/igh_data_sync/` - Update imports
8. All test files in `tests/` - Update imports
9. `README.md` - Update installation and usage instructions
10. `CLAUDE.md` - Update architecture documentation

## Migration Strategy

**Order of operations:**
1. Create new git branch: `git checkout -b package-restructure`
2. Create new directory structure (src/igh_data_sync/)
3. Copy files from lib/ to src/igh_data_sync/
4. Move configs to src/igh_data_sync/data/
5. Create CLI script files in src/igh_data_sync/scripts/ with new CLI arguments
6. Update all imports in copied files
7. Update config.py to support CLI parameters and new loading logic
8. Update pyproject.toml with full configuration
9. Create .python-version file
10. Update .gitignore with build artifacts (already present)
11. Update tests imports and fix Python 3.9 compatibility
12. Run tests to verify everything works
13. Update documentation (README.md, CLAUDE.md)
14. Clean up old files (scripts, lib/, requirements.txt, configs)
15. Commit changes with descriptive message

**No backward compatibility:**
- Root-level config files will be deleted
- Users must use CLI parameters to override package defaults
- .env can be in working directory or specified via --env-file parameter

## Verification Steps

### 1. Installation Test
```bash
# Clean environment
rm -rf .venv dist *.egg-info

# Install with UV
uv venv
source .venv/bin/activate
uv sync
uv sync --all-extras

# Verify commands are available
which sync-dataverse
which validate-schema
which generate-optionset-config
```

### 2. CLI Command Test
```bash
# Run help for each command
sync-dataverse --help
validate-schema --help
generate-optionset-config --help

# Test with actual data (if credentials available)
validate-schema --db-type sqlite
```

### 3. Import Test
```python
# Python REPL
from igh_data_sync import config
from igh_data_sync.sync import database
from igh_data_sync.validation import validator

# Verify version
import igh_data_sync
print(igh_data_sync.__version__)
```

### 4. Test Suite
```bash
# Run all tests
uv run pytest

# Run with coverage
uv run pytest --cov=src/igh_data_sync --cov-report=term-missing

# Verify no import errors
uv run pytest -v --collect-only
```

### 5. Config Loading Test
```bash
# Test package defaults (no config files in working dir)
cd /tmp
sync-dataverse --help  # Should use packaged config from src/igh_data_sync/data/

# Test custom config via CLI parameter
sync-dataverse --entities-config /path/to/custom/entities_config.json

# Test .env loading from working directory
cd /home/zuhdil/akvo/igh-data-sync
echo "DATAVERSE_API_URL=https://test.api.com" > .env
sync-dataverse --help  # Should load .env from working dir

# Test custom .env via CLI parameter
sync-dataverse --env-file /path/to/custom/.env

# Test system environment variables (no .env file)
cd /tmp
export DATAVERSE_API_URL=https://test.api.com
sync-dataverse --help  # Should use system env vars
```

### 6. Pre-commit Hooks
```bash
# Verify hooks still work
pre-commit run --all-files

# Test commit with changes
git add .
git commit -m "test: verify pre-commit hooks"
```

### 7. Build and Distribution Test
```bash
# Build wheel and sdist
uv build

# Verify build artifacts
ls dist/
# Should see: igh_data_sync-0.1.0-py3-none-any.whl, igh-data-sync-0.1.0.tar.gz

# Test installation from wheel
pip install dist/igh_data_sync-0.1.0-py3-none-any.whl
```

## Success Criteria

- ✅ Package installs via `uv sync` or `pip install .`
- ✅ All three CLI commands are available after installation
- ✅ Commands run without import errors
- ✅ All 121 tests pass
- ✅ Coverage remains at 65%+
- ✅ Pre-commit hooks pass
- ✅ Config files load from package data by default
- ✅ CLI parameters override config file locations (--entities-config, --optionsets-config)
- ✅ Environment variables load with correct precedence (--env-file → .env in working dir → system vars)
- ✅ Documentation updated with CLI parameter usage
- ✅ Build produces valid wheel and sdist
- ✅ No breaking changes to core library logic
- ✅ Old files cleaned up (lib/, config files at root, requirements.txt)
- ✅ Python 3.9 compatibility verified

## Key Design Decisions

### 1. src/ Layout

**Why:**
- Modern Python packaging standard
- Better isolation during testing
- Clear separation between source and tests
- Cleaner namespace management
- Easier to package and distribute

**Alternative considered:** Flat layout with package at root
- Rejected: Can cause import issues, mixing of source and artifacts

### 2. Hatchling Build Backend

**Why:**
- Modern PEP 517 build backend
- Simpler configuration than setuptools
- Automatically discovers packages in src/ layout
- Better integration with modern tools

**Alternative considered:** setuptools
- Rejected: More complex, older tooling

### 3. CLI Entry Points via setuptools

**Why:**
- Standard mechanism for installing console scripts
- Automatic PATH integration
- Cross-platform compatibility
- No need for shell scripts

**Alternative considered:** Direct script installation
- Rejected: Platform-specific, less portable

### 4. Configuration in Package Data

**Why:**
- Default configs always available
- No need for separate config installation
- Users can override via CLI parameters
- Cleaner for distribution

**Alternative considered:** External config files required
- Rejected: Poor user experience, complicates installation

### 5. No Backward Compatibility

**Why:**
- Simplifies implementation
- Clean break from script-based approach
- Users already need to reinstall/update
- Minimal migration cost (delete old files)

**Alternative considered:** Support both old and new structures
- Rejected: Adds complexity, confusion

## Notes

- All core library code remains functionally identical
- Only structural and import changes, no logic changes
- SCD2 temporal tracking, filtered sync, and all features preserved
- .env files can be in working directory or specified via CLI parameter
- Database paths continue to work via environment variables or CLI parameters
- Database schemas and sync behavior unchanged
- No backward compatibility with old config file locations at root level
- Default configs are packaged, but users can override via CLI parameters
- Python 3.9 requires `Optional` type hints instead of `|` operator
