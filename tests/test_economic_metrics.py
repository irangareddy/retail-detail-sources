"""Test the economic metrics models."""

from retail_data_sources.fred.models.economic_metrics import (
    EconomicData,
    EconomicMetric,
    MonthlyEconomicIndicators,
)

RANDOM_METRIC_VALUE = 100.0


def test_economic_metric() -> None:
    """Test the EconomicMetric dataclass."""
    metric = EconomicMetric(
        value=100.0,
        category="Confidence",
        description="Consumer confidence index",
        impact="High",
        label="CCI",
    )
    assert metric.value == RANDOM_METRIC_VALUE
    assert metric.category == "Confidence"
    assert metric.description == "Consumer confidence index"
    assert metric.impact == "High"
    assert metric.label == "CCI"


def test_monthly_economic_indicators() -> None:
    """Test the MonthlyEconomicIndicators dataclass."""
    metric = EconomicMetric(
        value=100.0,
        category="Confidence",
        description="Consumer confidence index",
        impact="High",
        label="CCI",
    )
    indicators = MonthlyEconomicIndicators(
        date="2023-10",
        consumer_confidence=metric,
        unemployment_rate=metric,
        inflation_rate=metric,
        gdp_growth_rate=metric,
        federal_funds_rate=metric,
        retail_sales=metric,
    )
    assert indicators.date == "2023-10"
    assert indicators.consumer_confidence == metric
    assert indicators.unemployment_rate == metric
    assert indicators.inflation_rate == metric
    assert indicators.gdp_growth_rate == metric
    assert indicators.federal_funds_rate == metric
    assert indicators.retail_sales == metric

    indicators_dict = indicators.to_dict()
    assert indicators_dict["date"] == "2023-10"
    assert "consumer_confidence" in indicators_dict["metrics"]

    snowflake_record = indicators.to_snowflake_record()
    assert snowflake_record["DATE"] == "2023-10"
    assert "CONSUMER_CONFIDENCE_VALUE" in snowflake_record


def test_economic_data() -> None:
    """Test the EconomicData dataclass."""
    metric = EconomicMetric(
        value=100.0,
        category="Confidence",
        description="Consumer confidence index",
        impact="High",
        label="CCI",
    )
    indicators = MonthlyEconomicIndicators(
        date="2023-10",
        consumer_confidence=metric,
        unemployment_rate=metric,
        inflation_rate=metric,
        gdp_growth_rate=metric,
        federal_funds_rate=metric,
        retail_sales=metric,
    )
    data = EconomicData(metrics=[indicators])
    assert len(data.metrics) == 1
    assert data.metrics[0] == indicators

    data_dict = {
        "2023-10": {
            "consumer_confidence": {
                "value": 100.0,
                "category": "Confidence",
                "description": "Consumer confidence index",
                "impact": "High",
                "label": "CCI",
            },
            "unemployment_rate": {
                "value": 100.0,
                "category": "Confidence",
                "description": "Consumer confidence index",
                "impact": "High",
                "label": "CCI",
            },
            "inflation_rate": {
                "value": 100.0,
                "category": "Confidence",
                "description": "Consumer confidence index",
                "impact": "High",
                "label": "CCI",
            },
            "gdp_growth_rate": {
                "value": 100.0,
                "category": "Confidence",
                "description": "Consumer confidence index",
                "impact": "High",
                "label": "CCI",
            },
            "federal_funds_rate": {
                "value": 100.0,
                "category": "Confidence",
                "description": "Consumer confidence index",
                "impact": "High",
                "label": "CCI",
            },
            "retail_sales": {
                "value": 100.0,
                "category": "Confidence",
                "description": "Consumer confidence index",
                "impact": "High",
                "label": "CCI",
            },
        }
    }

    economic_data = EconomicData.from_dict(data_dict)
    assert len(economic_data.metrics) == 1
    assert economic_data.metrics[0].date == "2023-10"

    snowflake_records = economic_data.to_snowflake_records()
    assert len(snowflake_records) == 1
    assert snowflake_records[0]["DATE"] == "2023-10"
