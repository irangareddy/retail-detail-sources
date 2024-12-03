"""Census SQL generation module for retail sales data."""

from datetime import datetime
from typing import Any


class CensusSnowflake:
    """Handles SQL generation for Census retail sales data."""

    @staticmethod
    def generate_create_tables_sql(schema: str, table_prefix: str) -> str:
        """Generate SQL to create necessary tables for retail sales data."""
        return f"""
        -- Metadata table
        CREATE TABLE IF NOT EXISTS {schema}.{table_prefix}_metadata (
            last_updated TIMESTAMP_NTZ NOT NULL,
            category_code VARCHAR(10) NOT NULL,
            category_description VARCHAR(255) NOT NULL,
            PRIMARY KEY (category_code)
        );

        -- Sales data table
        CREATE TABLE IF NOT EXISTS {schema}.{table_prefix}_sales (
            month VARCHAR(7) NOT NULL,
            state_code VARCHAR(2) NOT NULL,
            category_code VARCHAR(10) NOT NULL,
            sales_value DECIMAL(20, 2),
            state_share DECIMAL(10, 6),
            inserted_at TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
            PRIMARY KEY (month, state_code, category_code)
        );

        -- National totals table
        CREATE TABLE IF NOT EXISTS {schema}.{table_prefix}_national_totals (
            month VARCHAR(7) NOT NULL,
            category_code VARCHAR(10) NOT NULL,
            total_sales DECIMAL(20, 2) NOT NULL,
            inserted_at TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
            PRIMARY KEY (month, category_code)
        );
        """

    @staticmethod
    def prepare_metadata_records(metadata: dict[str, Any]) -> list[dict[str, Any]]:
        """Convert metadata into a list of records for loading."""
        records = []
        for category_code, category_desc in metadata["categories"].items():
            records.append(
                {
                    "last_updated": datetime.fromisoformat(metadata["last_updated"]),
                    "category_code": category_code,
                    "category_desc": category_desc,
                }
            )
        return records

    @staticmethod
    def generate_metadata_merge_sql(target_table: str) -> str:
        """Generate MERGE SQL statement for loading metadata."""
        return f"""
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

    @staticmethod
    def prepare_sales_records(sales_data: dict[str, Any]) -> list[dict[str, Any]]:
        """Convert sales data into a list of records for loading."""
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
        return records

    @staticmethod
    def generate_sales_merge_sql(target_table: str) -> str:
        """Generate MERGE SQL statement for loading sales data."""
        return f"""
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

    def prepare_load_sql(
        self,
        retail_report: dict[str, Any],
        target_schema: str,
        target_table: str,
    ) -> tuple[str, dict[str, Any]]:
        """Prepare SQL and data for loading retail sales data.

        Args:
            retail_report: Dictionary containing retail report data
            target_schema: Target schema name
            target_table: Target table prefix name

        Returns:
            tuple containing:
            - create tables SQL
            - dictionary containing:
                - metadata_records: list of metadata records
                - metadata_merge_sql: SQL for merging metadata
                - sales_records: list of sales records
                - sales_merge_sql: SQL for merging sales data

        """
        # Generate create tables SQL
        create_sql = self.generate_create_tables_sql(target_schema, target_table)

        # Prepare metadata
        metadata_records = self.prepare_metadata_records(retail_report["metadata"])
        metadata_merge_sql = self.generate_metadata_merge_sql(
            f"{target_schema}.{target_table}_metadata"
        )

        # Prepare sales data
        sales_records = self.prepare_sales_records(retail_report["sales_data"])
        sales_merge_sql = self.generate_sales_merge_sql(f"{target_schema}.{target_table}_sales")

        return create_sql, {
            "metadata_records": metadata_records,
            "metadata_merge_sql": metadata_merge_sql,
            "sales_records": sales_records,
            "sales_merge_sql": sales_merge_sql,
        }
