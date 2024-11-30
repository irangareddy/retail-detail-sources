"""Models for economic metrics."""

from dataclasses import dataclass


@dataclass
class Metric:
    """Base model for a single economic metric."""

    value: float | None
    category: str
    description: str
    impact: str
    label: str


@dataclass
class EconomicMetrics:
    """Monthly economic indicators."""

    date: str  # YYYY-MM
    consumer_confidence: Metric
    unemployment_rate: Metric
    inflation_rate: Metric
    gdp_growth_rate: Metric
    federal_funds_rate: Metric
    retail_sales: Metric

    def to_dict(self) -> dict:
        """Convert to dictionary format."""
        return {
            "date": self.date,
            "metrics": {
                "consumer_confidence": vars(self.consumer_confidence),
                "unemployment_rate": vars(self.unemployment_rate),
                "inflation_rate": vars(self.inflation_rate),
                "gdp_growth_rate": vars(self.gdp_growth_rate),
                "federal_funds_rate": vars(self.federal_funds_rate),
                "retail_sales": vars(self.retail_sales),
            },
        }

    def to_snowflake_record(self) -> dict:
        """Convert to flattened Snowflake record."""
        record = {"DATE": self.date}

        for metric_name, metric in vars(self).items():
            if metric_name != "date":
                prefix = metric_name.upper()
                metric_dict = vars(metric)
                for field, value in metric_dict.items():
                    record[f"{prefix}_{field.upper()}"] = value

        return record


@dataclass
class FREDData:
    """Collection of monthly economic metrics."""

    metrics: list[EconomicMetrics]

    @classmethod
    def from_dict(cls, data: dict) -> "FREDData":
        """Create FREDData from dictionary format."""
        metrics_list = []
        for date, metrics in data.items():
            monthly_metrics = EconomicMetrics(
                date=date,
                consumer_confidence=Metric(**metrics["consumer_confidence"]),
                unemployment_rate=Metric(**metrics["unemployment_rate"]),
                inflation_rate=Metric(**metrics["inflation_rate"]),
                gdp_growth_rate=Metric(**metrics["gdp_growth_rate"]),
                federal_funds_rate=Metric(**metrics["federal_funds_rate"]),
                retail_sales=Metric(**metrics["retail_sales"]),
            )
            metrics_list.append(monthly_metrics)
        return cls(metrics=sorted(metrics_list, key=lambda x: x.date))

    def to_snowflake_records(self) -> list[dict]:
        """Convert all metrics to Snowflake format."""
        return [metric.to_snowflake_record() for metric in self.metrics]
