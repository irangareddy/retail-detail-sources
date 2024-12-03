"""SQL generation for OpenWeather data."""

from dataclasses import asdict
from typing import Any

from retail_data_sources.openweather.models.state_weather import StateWeather


class OpenWeatherSnowflake:
    """Handles SQL generation for OpenWeather data."""

    @staticmethod
    def generate_create_table_sql(schema: str, table: str) -> str:
        """Generate SQL to create weather statistics table."""
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

        return f"""
        CREATE TABLE IF NOT EXISTS {schema}.{table} (
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

    @staticmethod
    def prepare_weather_records(weather_data: list[StateWeather]) -> list[dict[str, Any]]:
        """Convert weather data into a list of records for loading."""
        records = []

        for state_weather in weather_data:
            for month, monthly_stats in state_weather.monthly_weather.items():
                record = {
                    "STATE_NAME": state_weather.state_name,
                    "MONTH": month,
                    "SUNSHINE_HOURS_TOTAL": monthly_stats.sunshine_hours_total,
                }

                # Process each weather metric
                for metric_name in [
                    "temp",
                    "pressure",
                    "humidity",
                    "wind",
                    "precipitation",
                    "clouds",
                ]:
                    metric_stats = getattr(monthly_stats, metric_name)
                    prefix = "TEMPERATURE" if metric_name == "temp" else metric_name.upper()

                    metric_dict = asdict(metric_stats)
                    for field_name, value in metric_dict.items():
                        column_name = f"{prefix}_{field_name.upper()}"
                        record[column_name] = value

                records.append(record)

        return records

    @staticmethod
    def generate_merge_sql(target_table: str) -> str:
        """Generate MERGE SQL statement for loading weather data."""
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

        column_types = {
            col: "INTEGER" if col.endswith("_NUM") else "FLOAT" for col in all_columns[2:]
        }
        column_types["SUNSHINE_HOURS_TOTAL"] = "FLOAT"

        return f"""
        MERGE INTO {target_table} AS target
        USING (
            SELECT
                %(STATE_NAME)s as STATE_NAME,
                %(MONTH)s as MONTH,
                {', '.join(f'%({col})s::{column_types.get(col, "FLOAT")} as {col}'
                          for col in all_columns[2:])}
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

    def prepare_load_sql(
        self,
        weather_data: list[StateWeather],
        target_schema: str,
        target_table: str,
    ) -> tuple[str, list[dict[str, Any]], str]:
        """Prepare SQL and data for loading weather statistics.

        Returns:
            tuple containing:
            - create table SQL
            - list of record dictionaries for loading
            - merge SQL statement

        """
        # Create tables SQL
        create_sql = self.generate_create_table_sql(target_schema, target_table)

        # Transform the data
        records = self.prepare_weather_records(weather_data)

        # Generate merge SQL
        merge_sql = self.generate_merge_sql(f"{target_schema}.{target_table}")

        return create_sql, records, merge_sql
