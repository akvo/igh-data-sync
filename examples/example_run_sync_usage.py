#!/usr/bin/env python3
"""
Example usage of the run_sync() programmatic API.

This demonstrates how to use run_sync from Apache Airflow DAGs or other Python scripts.
"""

import asyncio
import logging

from igh_data_sync import run_sync
from igh_data_sync.config import Config


async def main():
    """Example of using run_sync programmatically."""
    # Setup logging
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
    )
    logger = logging.getLogger(__name__)

    # Create configuration
    # In production, load these from environment variables or secrets management
    config = Config(
        api_url="https://your-org.api.crm.dynamics.com/api/data/v9.2/",
        client_id="your-client-id",
        client_secret="your-client-secret",
        scope="https://your-org.crm.dynamics.com/.default",
        sqlite_db_path="dataverse.db",
    )

    try:
        # Run sync with default configuration (loads from package data)
        logger.info("Starting Dataverse sync...")

        success = await run_sync(
            config=config,
            verify_reference=True,  # Verify foreign key integrity
            logger=logger,  # Pass logger for integrated logging
        )

        if success:
            logger.info("✓ Sync completed successfully!")
            return 0
        else:
            logger.error("❌ Sync failed - check logs for details")
            return 1

    except RuntimeError as e:
        logger.error(f"Authentication failed: {e}")
        return 1
    except Exception as e:
        logger.exception(f"Unexpected error: {e}")
        return 1


async def airflow_dag_example():
    """
    Example of how to use run_sync in an Apache Airflow DAG.

    In your Airflow DAG, you would use this with PythonOperator:

    from airflow import DAG
    from airflow.operators.python import PythonOperator
    from datetime import datetime

    def sync_dataverse_task(**context):
        import asyncio
        from igh_data_sync import run_sync
        from igh_data_sync.config import Config

        # Get config from Airflow Variables or Connections
        config = Config(
            api_url=Variable.get("DATAVERSE_API_URL"),
            client_id=Variable.get("DATAVERSE_CLIENT_ID"),
            client_secret=Variable.get("DATAVERSE_CLIENT_SECRET"),
            scope=Variable.get("DATAVERSE_SCOPE"),
            sqlite_db_path="/data/dataverse.db",
        )

        # Run sync
        success = asyncio.run(run_sync(
            config=config,
            verify_reference=True,
            logger=context['task_instance'].log,
        ))

        if not success:
            raise Exception("Dataverse sync failed")

    with DAG(
        'dataverse_sync',
        start_date=datetime(2024, 1, 1),
        schedule_interval='@daily',
    ) as dag:
        sync_task = PythonOperator(
            task_id='sync_dataverse',
            python_callable=sync_dataverse_task,
        )
    """
    pass


if __name__ == "__main__":
    exit_code = asyncio.run(main())
    exit(exit_code)
