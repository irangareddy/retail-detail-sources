"""Models for economic metrics with enhanced dictionary support."""

from dataclasses import dataclass, fields
from typing import Any


@dataclass
class EconomicMetric:
    """Base model for a single economic metric."""

    value: float | None
    category: str
    description: str
    impact: str
    label: str

    def to_dict(self) -> dict[str, Any]:
        """Convert to dictionary with proper handling of None values."""
        return {
            "value": self.value if self.value is not None else None,
            "category": self.category,
            "description": self.description,
            "impact": self.impact,
            "label": self.label,
        }

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> "EconomicMetric":
        """Create instance from dictionary."""
        return cls(
            value=data.get("value"),
            category=data["category"],
            description=data["description"],
            impact=data["impact"],
            label=data["label"],
        )


@dataclass
class MonthlyEconomicIndicators:
    """Monthly economic indicators."""

    date: str  # YYYY-MM
    consumer_confidence: EconomicMetric
    unemployment_rate: EconomicMetric
    inflation_rate: EconomicMetric
    gdp_growth_rate: EconomicMetric
    federal_funds_rate: EconomicMetric
    retail_sales: EconomicMetric

    def to_dict(self) -> dict[str, Any]:
        """Convert to nested dictionary format."""
        return {
            "date": self.date,
            "metrics": {
                "consumer_confidence": self.consumer_confidence.to_dict(),
                "unemployment_rate": self.unemployment_rate.to_dict(),
                "inflation_rate": self.inflation_rate.to_dict(),
                "gdp_growth_rate": self.gdp_growth_rate.to_dict(),
                "federal_funds_rate": self.federal_funds_rate.to_dict(),
                "retail_sales": self.retail_sales.to_dict(),
            },
        }

    def to_snowflake_record(self) -> dict[str, Any]:
        """Convert to flattened Snowflake record with proper typing."""
        record: dict[str, Any] = {"DATE": self.date}

        for metric_name, metric in {
            "CONSUMER_CONFIDENCE": self.consumer_confidence,
            "UNEMPLOYMENT_RATE": self.unemployment_rate,
            "INFLATION_RATE": self.inflation_rate,
            "GDP_GROWTH_RATE": self.gdp_growth_rate,
            "FEDERAL_FUNDS_RATE": self.federal_funds_rate,
            "RETAIL_SALES": self.retail_sales,
        }.items():
            metric_dict = metric.to_dict()
            for field, value in metric_dict.items():
                column_name = f"{metric_name}_{field.upper()}"
                record[column_name] = value

        return record

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> "MonthlyEconomicIndicators":
        """Create instance from dictionary."""
        metrics = data.get("metrics", {})
        return cls(
            date=data["date"],
            consumer_confidence=EconomicMetric.from_dict(metrics["consumer_confidence"]),
            unemployment_rate=EconomicMetric.from_dict(metrics["unemployment_rate"]),
            inflation_rate=EconomicMetric.from_dict(metrics["inflation_rate"]),
            gdp_growth_rate=EconomicMetric.from_dict(metrics["gdp_growth_rate"]),
            federal_funds_rate=EconomicMetric.from_dict(metrics["federal_funds_rate"]),
            retail_sales=EconomicMetric.from_dict(metrics["retail_sales"]),
        )


@dataclass
class EconomicData:
    """Collection of monthly economic metrics."""

    metrics: list[MonthlyEconomicIndicators]

    def to_dict(self) -> dict[str, Any]:
        """Convert entire dataset to dictionary."""
        return {"metrics": [metric.to_dict() for metric in self.metrics]}

    def to_snowflake_records(self) -> list[dict[str, Any]]:
        """Convert all metrics to Snowflake format."""
        return [metric.to_snowflake_record() for metric in self.metrics]

    @classmethod
    def from_dict(cls, data: dict[str, dict[str, Any]]) -> "EconomicData":
        """Create EconomicData from dictionary format."""
        metrics_list = []
        for date, metrics in data.items():
            monthly_metrics = MonthlyEconomicIndicators(
                date=date,
                consumer_confidence=EconomicMetric(**metrics["consumer_confidence"]),
                unemployment_rate=EconomicMetric(**metrics["unemployment_rate"]),
                inflation_rate=EconomicMetric(**metrics["inflation_rate"]),
                gdp_growth_rate=EconomicMetric(**metrics["gdp_growth_rate"]),
                federal_funds_rate=EconomicMetric(**metrics["federal_funds_rate"]),
                retail_sales=EconomicMetric(**metrics["retail_sales"]),
            )
            metrics_list.append(monthly_metrics)
        return cls(metrics=sorted(metrics_list, key=lambda x: x.date))

    def get_snowflake_schema(self) -> str:
        """Generate Snowflake table creation SQL."""
        schema = ["DATE VARCHAR(7)"]

        # Get field names from EconomicMetric
        metric_fields = [f.name.upper() for f in fields(EconomicMetric)]

        # Generate columns for each metric type
        metric_types = [
            "CONSUMER_CONFIDENCE",
            "UNEMPLOYMENT_RATE",
            "INFLATION_RATE",
            "GDP_GROWTH_RATE",
            "FEDERAL_FUNDS_RATE",
            "RETAIL_SALES",
        ]

        for metric in metric_types:
            for field in metric_fields:
                column_type = "FLOAT" if field == "VALUE" else "VARCHAR"
                schema.append(f"{metric}_{field} {column_type}")

        return f"CREATE OR REPLACE TABLE ECONOMIC_METRICS (\n  {',\n  '.join(schema)}\n)"
