"""Open Weather API data source."""

import logging
from dataclasses import asdict
from typing import Any

from openweather.models.state_weather import StateWeather
from snowflake.database_cursor import DatabaseCursor


def load_weather_statistics(
    cursor: DatabaseCursor,
    weather_data: list[StateWeather],
    target_schema: str,
    target_table: str,
    batch_size: int = 1000,
) -> None:
    """Load weather statistics data into Snowflake.

    Args:
        cursor: Snowflake cursor
        weather_data: List of StateWeather objects
        target_schema: Target schema name
        target_table: Target table base name
        batch_size: Number of records to process in each batch

    """
    try:
        cursor.execute("BEGIN")

        # Create tables if they don't exist
        _create_weather_tables(cursor, target_schema, target_table)

        # Transform and load the data
        records = _prepare_weather_records(weather_data)
        _batch_load_records(cursor, records, f"{target_schema}.{target_table}", batch_size)

        cursor.execute("COMMIT")
        logging.info(f"Successfully loaded weather statistics into {target_schema}.{target_table}")

    except Exception:
        cursor.execute("ROLLBACK")
        logging.exception("Error loading weather statistics")
        raise


def _create_weather_tables(cursor: DatabaseCursor, schema: str, table: str) -> None:
    """Create the necessary table for weather statistics."""
    metric_columns = """
        {prefix}_RECORD_MIN FLOAT,
        {prefix}_RECORD_MAX FLOAT,
        {prefix}_AVERAGE_MIN FLOAT,
        {prefix}_AVERAGE_MAX FLOAT,
        {prefix}_MEDIAN FLOAT,
        {prefix}_MEAN FLOAT,
        {prefix}_P25 FLOAT,
        {prefix}_P75 FLOAT,
        {prefix}_ST_DEV FLOAT,
        {prefix}_NUM INTEGER
    """

    create_table_sql = f"""
    CREATE TABLE IF NOT EXISTS {{schema}}.{{table}} (
        STATE_NAME VARCHAR(100) NOT NULL,
        MONTH INTEGER NOT NULL,

        {metric_columns.format(prefix='TEMPERATURE')},
        {metric_columns.format(prefix='PRESSURE')},
        {metric_columns.format(prefix='HUMIDITY')},
        {metric_columns.format(prefix='WIND')},
        {metric_columns.format(prefix='PRECIPITATION')},
        {metric_columns.format(prefix='CLOUDS')},

        SUNSHINE_HOURS_TOTAL FLOAT,
        INSERTED_AT TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),

        PRIMARY KEY (STATE_NAME, MONTH)
    )
    """

    cursor.execute(create_table_sql.format(schema=schema, table=table))


def _prepare_weather_records(weather_data: list[StateWeather]) -> list[dict[str, Any]]:
    """Convert weather data into a list of flattened records for loading."""
    records = []

    for state_weather in weather_data:
        for month, monthly_stats in state_weather.monthly_weather.items():
            record = {
                "STATE_NAME": state_weather.state_name,
                "MONTH": month,
                "SUNSHINE_HOURS_TOTAL": monthly_stats.sunshine_hours_total,
            }

            # Process each weather metric
            for metric_name in ["temp", "pressure", "humidity", "wind", "precipitation", "clouds"]:
                metric_stats = getattr(monthly_stats, metric_name)
                prefix = metric_name.upper()
                if metric_name == "temp":
                    prefix = "TEMPERATURE"

                metric_dict = asdict(metric_stats)
                for field_name, value in metric_dict.items():
                    column_name = f"{prefix}_{field_name.upper()}"
                    record[column_name] = value

            records.append(record)

    return records


def _batch_load_records(
    cursor: DatabaseCursor, records: list[dict[str, Any]], target_table: str, batch_size: int
) -> None:
    """Load records in batches using MERGE statement."""
    # Generate column lists for merge statement
    metric_names = ["TEMPERATURE", "PRESSURE", "HUMIDITY", "WIND", "PRECIPITATION", "CLOUDS"]
    stat_fields = [
        "RECORD_MIN",
        "RECORD_MAX",
        "AVERAGE_MIN",
        "AVERAGE_MAX",
        "MEDIAN",
        "MEAN",
        "P25",
        "P75",
        "ST_DEV",
        "NUM",
    ]

    all_columns = ["STATE_NAME", "MONTH"]
    update_sets = []
    value_items = ["source.STATE_NAME", "source.MONTH"]

    # Add metric columns
    for metric in metric_names:
        for field in stat_fields:
            col_name = f"{metric}_{field}"
            all_columns.append(col_name)
            update_sets.append(f"{col_name} = source.{col_name}")
            value_items.append(f"source.{col_name}")

    # Add sunshine hours
    all_columns.append("SUNSHINE_HOURS_TOTAL")
    update_sets.append("SUNSHINE_HOURS_TOTAL = source.SUNSHINE_HOURS_TOTAL")
    value_items.append("source.SUNSHINE_HOURS_TOTAL")

    # Build the merge statement
    merge_sql = f"""
    MERGE INTO {{target_table}} AS target
    USING (
        SELECT
            %(STATE_NAME)s as STATE_NAME,
            %(MONTH)s as MONTH,
            {', '.join(f'%({col})s::{_get_column_type(col)} as {col}' for col in all_columns[2:])}
    ) AS source
    ON target.STATE_NAME = source.STATE_NAME AND target.MONTH = source.MONTH
    WHEN MATCHED THEN
        UPDATE SET
            {', '.join(update_sets)},
            INSERTED_AT = CURRENT_TIMESTAMP()
    WHEN NOT MATCHED THEN
        INSERT ({', '.join(all_columns)})
        VALUES ({', '.join(value_items)})
    """

    # Process records in batches
    for i in range(0, len(records), batch_size):
        batch = records[i : i + batch_size]
        for record in batch:
            cursor.execute(merge_sql.format(target_table=target_table), record)


def _get_column_type(column_name: str) -> str:
    """Get the Snowflake column type for parameter binding."""
    if column_name.endswith("_NUM"):
        return "INTEGER"
    if column_name == "SUNSHINE_HOURS_TOTAL":
        return "FLOAT"
    return "FLOAT"
