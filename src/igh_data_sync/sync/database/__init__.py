"""SQLite database operations for sync.

This package provides database management for the sync process, including:
- DatabaseManager: Core database connection and query management
- SCD2Result: Result type for SCD2 upsert operations
- OptionSetStorage: Option set and junction table operations
- SCD2Upserter: SCD2 and batch upsert logic
"""

from .manager import DatabaseManager, SCD2Result
from .optionset_storage import OptionSetStorage
from .scd2_upsert import SCD2Upserter

__all__ = ["DatabaseManager", "OptionSetStorage", "SCD2Result", "SCD2Upserter"]
