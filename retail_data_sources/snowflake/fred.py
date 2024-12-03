"""Fred Snowflake data source module."""

import logging
from typing import Any

from snowflake.database_cursor import DatabaseCursor

from retail_data_sources.fred.models.economic_metrics import EconomicData


def load_economic_metrics(
    cursor: DatabaseCursor,
    economic_data: dict[str, Any],
    target_schema: str,
    target_table: str,
    batch_size: int = 1000,
) -> None:
    """Load FRED economic metrics data into Snowflake.

    Args:
        cursor: Snowflake cursor
        economic_data: Dictionary containing economic metrics
        target_schema: Target schema name
        target_table: Target table base name
        batch_size: Number of records to process in each batch

    """
    try:
        cursor.execute("BEGIN")

        # Create tables if they don't exist
        _create_economic_tables(cursor, target_schema, target_table)

        # Load the data
        records = _prepare_economic_records(economic_data)
        _batch_load_records(cursor, records, f"{target_schema}.{target_table}", batch_size)

        cursor.execute("COMMIT")
        logging.info(f"Successfully loaded economic metrics into {target_schema}.{target_table}")

    except Exception:
        cursor.execute("ROLLBACK")
        logging.exception("Error loading economic metrics")
        raise


def _create_economic_tables(cursor: DatabaseCursor, schema: str, table: str) -> None:
    """Create the necessary table for economic metrics."""
    create_table_sql = """
    CREATE TABLE IF NOT EXISTS {schema}.{table} (
        DATE VARCHAR(7) NOT NULL,

        CONSUMER_CONFIDENCE_VALUE FLOAT,
        CONSUMER_CONFIDENCE_CATEGORY VARCHAR(100),
        CONSUMER_CONFIDENCE_DESCRIPTION VARCHAR(500),
        CONSUMER_CONFIDENCE_IMPACT VARCHAR(100),
        CONSUMER_CONFIDENCE_LABEL VARCHAR(100),

        UNEMPLOYMENT_RATE_VALUE FLOAT,
        UNEMPLOYMENT_RATE_CATEGORY VARCHAR(100),
        UNEMPLOYMENT_RATE_DESCRIPTION VARCHAR(500),
        UNEMPLOYMENT_RATE_IMPACT VARCHAR(100),
        UNEMPLOYMENT_RATE_LABEL VARCHAR(100),

        INFLATION_RATE_VALUE FLOAT,
        INFLATION_RATE_CATEGORY VARCHAR(100),
        INFLATION_RATE_DESCRIPTION VARCHAR(500),
        INFLATION_RATE_IMPACT VARCHAR(100),
        INFLATION_RATE_LABEL VARCHAR(100),

        GDP_GROWTH_RATE_VALUE FLOAT,
        GDP_GROWTH_RATE_CATEGORY VARCHAR(100),
        GDP_GROWTH_RATE_DESCRIPTION VARCHAR(500),
        GDP_GROWTH_RATE_IMPACT VARCHAR(100),
        GDP_GROWTH_RATE_LABEL VARCHAR(100),

        FEDERAL_FUNDS_RATE_VALUE FLOAT,
        FEDERAL_FUNDS_RATE_CATEGORY VARCHAR(100),
        FEDERAL_FUNDS_RATE_DESCRIPTION VARCHAR(500),
        FEDERAL_FUNDS_RATE_IMPACT VARCHAR(100),
        FEDERAL_FUNDS_RATE_LABEL VARCHAR(100),

        RETAIL_SALES_VALUE FLOAT,
        RETAIL_SALES_CATEGORY VARCHAR(100),
        RETAIL_SALES_DESCRIPTION VARCHAR(500),
        RETAIL_SALES_IMPACT VARCHAR(100),
        RETAIL_SALES_LABEL VARCHAR(100),

        INSERTED_AT TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),

        PRIMARY KEY (DATE)
    )
    """

    cursor.execute(create_table_sql.format(schema=schema, table=table))


def _prepare_economic_records(economic_data: dict[str, Any]) -> list[dict[str, Any]]:
    """Convert economic data into a list of records for loading."""
    # Convert dictionary to EconomicData object
    data = EconomicData.from_dict(economic_data)

    # Use the built-in conversion method
    return data.to_snowflake_records()


def _batch_load_records(
    cursor: DatabaseCursor, records: list[dict[str, Any]], target_table: str, batch_size: int
) -> None:
    """Load records in batches using MERGE statement."""
    # Prepare the merge statement with all columns
    merge_sql = """
    MERGE INTO {target_table} AS target
    USING (
        SELECT
            %(DATE)s as DATE,

            %(CONSUMER_CONFIDENCE_VALUE)s::FLOAT as CONSUMER_CONFIDENCE_VALUE,
            %(CONSUMER_CONFIDENCE_CATEGORY)s as CONSUMER_CONFIDENCE_CATEGORY,
            %(CONSUMER_CONFIDENCE_DESCRIPTION)s as CONSUMER_CONFIDENCE_DESCRIPTION,
            %(CONSUMER_CONFIDENCE_IMPACT)s as CONSUMER_CONFIDENCE_IMPACT,
            %(CONSUMER_CONFIDENCE_LABEL)s as CONSUMER_CONFIDENCE_LABEL,

            %(UNEMPLOYMENT_RATE_VALUE)s::FLOAT as UNEMPLOYMENT_RATE_VALUE,
            %(UNEMPLOYMENT_RATE_CATEGORY)s as UNEMPLOYMENT_RATE_CATEGORY,
            %(UNEMPLOYMENT_RATE_DESCRIPTION)s as UNEMPLOYMENT_RATE_DESCRIPTION,
            %(UNEMPLOYMENT_RATE_IMPACT)s as UNEMPLOYMENT_RATE_IMPACT,
            %(UNEMPLOYMENT_RATE_LABEL)s as UNEMPLOYMENT_RATE_LABEL,

            %(INFLATION_RATE_VALUE)s::FLOAT as INFLATION_RATE_VALUE,
            %(INFLATION_RATE_CATEGORY)s as INFLATION_RATE_CATEGORY,
            %(INFLATION_RATE_DESCRIPTION)s as INFLATION_RATE_DESCRIPTION,
            %(INFLATION_RATE_IMPACT)s as INFLATION_RATE_IMPACT,
            %(INFLATION_RATE_LABEL)s as INFLATION_RATE_LABEL,

            %(GDP_GROWTH_RATE_VALUE)s::FLOAT as GDP_GROWTH_RATE_VALUE,
            %(GDP_GROWTH_RATE_CATEGORY)s as GDP_GROWTH_RATE_CATEGORY,
            %(GDP_GROWTH_RATE_DESCRIPTION)s as GDP_GROWTH_RATE_DESCRIPTION,
            %(GDP_GROWTH_RATE_IMPACT)s as GDP_GROWTH_RATE_IMPACT,
            %(GDP_GROWTH_RATE_LABEL)s as GDP_GROWTH_RATE_LABEL,

            %(FEDERAL_FUNDS_RATE_VALUE)s::FLOAT as FEDERAL_FUNDS_RATE_VALUE,
            %(FEDERAL_FUNDS_RATE_CATEGORY)s as FEDERAL_FUNDS_RATE_CATEGORY,
            %(FEDERAL_FUNDS_RATE_DESCRIPTION)s as FEDERAL_FUNDS_RATE_DESCRIPTION,
            %(FEDERAL_FUNDS_RATE_IMPACT)s as FEDERAL_FUNDS_RATE_IMPACT,
            %(FEDERAL_FUNDS_RATE_LABEL)s as FEDERAL_FUNDS_RATE_LABEL,

            %(RETAIL_SALES_VALUE)s::FLOAT as RETAIL_SALES_VALUE,
            %(RETAIL_SALES_CATEGORY)s as RETAIL_SALES_CATEGORY,
            %(RETAIL_SALES_DESCRIPTION)s as RETAIL_SALES_DESCRIPTION,
            %(RETAIL_SALES_IMPACT)s as RETAIL_SALES_IMPACT,
            %(RETAIL_SALES_LABEL)s as RETAIL_SALES_LABEL

    ) AS source
    ON target.DATE = source.DATE
    WHEN MATCHED THEN
        UPDATE SET
            CONSUMER_CONFIDENCE_VALUE = source.CONSUMER_CONFIDENCE_VALUE,
            CONSUMER_CONFIDENCE_CATEGORY = source.CONSUMER_CONFIDENCE_CATEGORY,
            CONSUMER_CONFIDENCE_DESCRIPTION = source.CONSUMER_CONFIDENCE_DESCRIPTION,
            CONSUMER_CONFIDENCE_IMPACT = source.CONSUMER_CONFIDENCE_IMPACT,
            CONSUMER_CONFIDENCE_LABEL = source.CONSUMER_CONFIDENCE_LABEL,

            UNEMPLOYMENT_RATE_VALUE = source.UNEMPLOYMENT_RATE_VALUE,
            UNEMPLOYMENT_RATE_CATEGORY = source.UNEMPLOYMENT_RATE_CATEGORY,
            UNEMPLOYMENT_RATE_DESCRIPTION = source.UNEMPLOYMENT_RATE_DESCRIPTION,
            UNEMPLOYMENT_RATE_IMPACT = source.UNEMPLOYMENT_RATE_IMPACT,
            UNEMPLOYMENT_RATE_LABEL = source.UNEMPLOYMENT_RATE_LABEL,

            INFLATION_RATE_VALUE = source.INFLATION_RATE_VALUE,
            INFLATION_RATE_CATEGORY = source.INFLATION_RATE_CATEGORY,
            INFLATION_RATE_DESCRIPTION = source.INFLATION_RATE_DESCRIPTION,
            INFLATION_RATE_IMPACT = source.INFLATION_RATE_IMPACT,
            INFLATION_RATE_LABEL = source.INFLATION_RATE_LABEL,

            GDP_GROWTH_RATE_VALUE = source.GDP_GROWTH_RATE_VALUE,
            GDP_GROWTH_RATE_CATEGORY = source.GDP_GROWTH_RATE_CATEGORY,
            GDP_GROWTH_RATE_DESCRIPTION = source.GDP_GROWTH_RATE_DESCRIPTION,
            GDP_GROWTH_RATE_IMPACT = source.GDP_GROWTH_RATE_IMPACT,
            GDP_GROWTH_RATE_LABEL = source.GDP_GROWTH_RATE_LABEL,

            FEDERAL_FUNDS_RATE_VALUE = source.FEDERAL_FUNDS_RATE_VALUE,
            FEDERAL_FUNDS_RATE_CATEGORY = source.FEDERAL_FUNDS_RATE_CATEGORY,
            FEDERAL_FUNDS_RATE_DESCRIPTION = source.FEDERAL_FUNDS_RATE_DESCRIPTION,
            FEDERAL_FUNDS_RATE_IMPACT = source.FEDERAL_FUNDS_RATE_IMPACT,
            FEDERAL_FUNDS_RATE_LABEL = source.FEDERAL_FUNDS_RATE_LABEL,

            RETAIL_SALES_VALUE = source.RETAIL_SALES_VALUE,
            RETAIL_SALES_CATEGORY = source.RETAIL_SALES_CATEGORY,
            RETAIL_SALES_DESCRIPTION = source.RETAIL_SALES_DESCRIPTION,
            RETAIL_SALES_IMPACT = source.RETAIL_SALES_IMPACT,
            RETAIL_SALES_LABEL = source.RETAIL_SALES_LABEL,

            INSERTED_AT = CURRENT_TIMESTAMP()
    WHEN NOT MATCHED THEN
        INSERT (
            DATE,
            CONSUMER_CONFIDENCE_VALUE, CONSUMER_CONFIDENCE_CATEGORY,
            CONSUMER_CONFIDENCE_DESCRIPTION, CONSUMER_CONFIDENCE_IMPACT,
            CONSUMER_CONFIDENCE_LABEL,

            UNEMPLOYMENT_RATE_VALUE, UNEMPLOYMENT_RATE_CATEGORY,
            UNEMPLOYMENT_RATE_DESCRIPTION, UNEMPLOYMENT_RATE_IMPACT,
            UNEMPLOYMENT_RATE_LABEL,

            INFLATION_RATE_VALUE, INFLATION_RATE_CATEGORY,
            INFLATION_RATE_DESCRIPTION, INFLATION_RATE_IMPACT,
            INFLATION_RATE_LABEL,

            GDP_GROWTH_RATE_VALUE, GDP_GROWTH_RATE_CATEGORY,
            GDP_GROWTH_RATE_DESCRIPTION, GDP_GROWTH_RATE_IMPACT,
            GDP_GROWTH_RATE_LABEL,

            FEDERAL_FUNDS_RATE_VALUE, FEDERAL_FUNDS_RATE_CATEGORY,
            FEDERAL_FUNDS_RATE_DESCRIPTION, FEDERAL_FUNDS_RATE_IMPACT,
            FEDERAL_FUNDS_RATE_LABEL,

            RETAIL_SALES_VALUE, RETAIL_SALES_CATEGORY,
            RETAIL_SALES_DESCRIPTION, RETAIL_SALES_IMPACT,
            RETAIL_SALES_LABEL
        )
        VALUES (
            source.DATE,
            source.CONSUMER_CONFIDENCE_VALUE, source.CONSUMER_CONFIDENCE_CATEGORY,
            source.CONSUMER_CONFIDENCE_DESCRIPTION, source.CONSUMER_CONFIDENCE_IMPACT,
            source.CONSUMER_CONFIDENCE_LABEL,

            source.UNEMPLOYMENT_RATE_VALUE, source.UNEMPLOYMENT_RATE_CATEGORY,
            source.UNEMPLOYMENT_RATE_DESCRIPTION, source.UNEMPLOYMENT_RATE_IMPACT,
            source.UNEMPLOYMENT_RATE_LABEL,

            source.INFLATION_RATE_VALUE, source.INFLATION_RATE_CATEGORY,
            source.INFLATION_RATE_DESCRIPTION, source.INFLATION_RATE_IMPACT,
            source.INFLATION_RATE_LABEL,

            source.GDP_GROWTH_RATE_VALUE, source.GDP_GROWTH_RATE_CATEGORY,
            source.GDP_GROWTH_RATE_DESCRIPTION, source.GDP_GROWTH_RATE_IMPACT,
            source.GDP_GROWTH_RATE_LABEL,

            source.FEDERAL_FUNDS_RATE_VALUE, source.FEDERAL_FUNDS_RATE_CATEGORY,
            source.FEDERAL_FUNDS_RATE_DESCRIPTION, source.FEDERAL_FUNDS_RATE_IMPACT,
            source.FEDERAL_FUNDS_RATE_LABEL,

            source.RETAIL_SALES_VALUE, source.RETAIL_SALES_CATEGORY,
            source.RETAIL_SALES_DESCRIPTION, source.RETAIL_SALES_IMPACT,
            source.RETAIL_SALES_LABEL
        )
    """

    # Process records in batches
    for i in range(0, len(records), batch_size):
        batch = records[i : i + batch_size]
        for record in batch:
            cursor.execute(merge_sql.format(target_table=target_table), record)
