"""Census data source for Snowflake."""

import logging
from datetime import datetime
from typing import Any

from snowflake.database_cursor import DatabaseCursor


def load_retail_sales(
    cursor: DatabaseCursor,
    retail_report: dict[str, Any],
    target_schema: str,
    target_table: str,
    batch_size: int = 1000,
) -> None:
    """Load retail sales data into a database using the provided cursor.

    Args:
        cursor: Database cursor with execute() method (e.g., Snowflake cursor)
        retail_report: Dictionary containing retail report data
        target_schema: Target schema name
        target_table: Target table name
        batch_size: Number of records to process in each batch

    Raises:
        Exception: If there's an error during the load process

    """
    try:
        cursor.execute("BEGIN")

        # Create tables if they don't exist
        _create_tables(cursor, target_schema, target_table)

        # Load metadata
        _load_metadata(
            cursor, retail_report["metadata"], f"{target_schema}.{target_table}_metadata"
        )

        # Load sales data
        _load_sales_data(
            cursor, retail_report["sales_data"], f"{target_schema}.{target_table}_sales", batch_size
        )

        cursor.execute("COMMIT")
        logging.info(f"Successfully loaded retail sales data into {target_schema}.{target_table}")

    except Exception:
        cursor.execute("ROLLBACK")
        logging.exception("Error loading retail sales data")
        raise


def _create_tables(cursor: DatabaseCursor, schema: str, table_prefix: str) -> None:
    """Create the necessary tables for storing retail sales data."""
    # Create metadata table
    cursor.execute(f"""
    CREATE TABLE IF NOT EXISTS {schema}.{table_prefix}_metadata (
        last_updated TIMESTAMP_NTZ NOT NULL,
        category_code VARCHAR(10) NOT NULL,
        category_description VARCHAR(255) NOT NULL,
        PRIMARY KEY (category_code)
    )
    """)

    # Create sales data table
    cursor.execute(f"""
    CREATE TABLE IF NOT EXISTS {schema}.{table_prefix}_sales (
        month VARCHAR(7) NOT NULL,
        state_code VARCHAR(2) NOT NULL,
        category_code VARCHAR(10) NOT NULL,
        sales_value DECIMAL(20, 2),
        state_share DECIMAL(10, 6),
        inserted_at TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
        PRIMARY KEY (month, state_code, category_code)
    )
    """)

    # Create national totals table
    cursor.execute(f"""
    CREATE TABLE IF NOT EXISTS {schema}.{table_prefix}_national_totals (
        month VARCHAR(7) NOT NULL,
        category_code VARCHAR(10) NOT NULL,
        total_sales DECIMAL(20, 2) NOT NULL,
        inserted_at TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
        PRIMARY KEY (month, category_code)
    )
    """)


def _load_metadata(cursor: DatabaseCursor, metadata: dict[str, Any], target_table: str) -> None:
    """Load metadata into the metadata table."""
    for category_code, category_desc in metadata["categories"].items():
        merge_sql = f"""
        MERGE INTO {target_table} AS target
        USING (
            SELECT
                %(last_updated)s::TIMESTAMP_NTZ as last_updated,
                %(category_code)s as category_code,
                %(category_desc)s as category_description
        ) AS source
        ON target.category_code = source.category_code
        WHEN MATCHED THEN
            UPDATE SET
                last_updated = source.last_updated,
                category_description = source.category_description
        WHEN NOT MATCHED THEN
            INSERT (last_updated, category_code, category_description)
            VALUES (source.last_updated, source.category_code, source.category_description)
        """

        cursor.execute(
            merge_sql,
            {
                "last_updated": datetime.fromisoformat(metadata["last_updated"]),
                "category_code": category_code,
                "category_desc": category_desc,
            },
        )


def _load_sales_data(
    cursor: DatabaseCursor, sales_data: dict[str, Any], target_table: str, batch_size: int
) -> None:
    """Load sales data into the sales table."""
    records = []
    for month, month_data in sales_data.items():
        # Process state-level data
        for state_code, state_data in month_data["states"].items():
            for category_code in ["445", "448"]:
                category_data = state_data.get(category_code)
                if category_data is not None:
                    records.append(
                        {
                            "month": month,
                            "state_code": state_code,
                            "category_code": category_code,
                            "sales_value": category_data["sales_value"],
                            "state_share": category_data["state_share"],
                        }
                    )

                # Process in batches
                if len(records) >= batch_size:
                    _batch_insert_sales(cursor, records, target_table)
                    records = []

    # Process any remaining records
    if records:
        _batch_insert_sales(cursor, records, target_table)


def _batch_insert_sales(cursor: DatabaseCursor, records: list, target_table: str) -> None:
    """Insert a batch of sales records using MERGE statement."""
    merge_sql = f"""
    MERGE INTO {target_table} AS target
    USING (
        SELECT
            %(month)s as month,
            %(state_code)s as state_code,
            %(category_code)s as category_code,
            %(sales_value)s::DECIMAL(20,2) as sales_value,
            %(state_share)s::DECIMAL(10,6) as state_share
    ) AS source
    ON target.month = source.month
        AND target.state_code = source.state_code
        AND target.category_code = source.category_code
    WHEN MATCHED THEN
        UPDATE SET
            sales_value = source.sales_value,
            state_share = source.state_share,
            inserted_at = CURRENT_TIMESTAMP()
    WHEN NOT MATCHED THEN
        INSERT (month, state_code, category_code, sales_value, state_share)
        VALUES (
            source.month, source.state_code, source.category_code,
            source.sales_value, source.state_share
        )
    """

    for record in records:
        cursor.execute(merge_sql, record)
