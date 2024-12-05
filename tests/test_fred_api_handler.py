"""Test the FRED API handler."""

import os
from pathlib import Path

import pytest

from retail_data_sources.fred.fred_api_handler import FREDAPIHandler
from retail_data_sources.utils.constants import SERIES_MAPPING
from tests.utils import needs_fred


@pytest.fixture
@needs_fred
def fred_handler(tmp_path: Path) -> FREDAPIHandler:
    """Create a FRED API handler for testing."""
    assert "FRED_API_KEY" in os.environ, "FRED_API_KEY must be set in the environment"
    return FREDAPIHandler(base_dir=str(tmp_path))


@needs_fred
def test_initialization(fred_handler: FREDAPIHandler) -> None:
    """Test initializing the FRED API handler."""
    assert fred_handler.api_key == os.environ["FRED_API_KEY"]
    assert Path.exists(Path(fred_handler.base_dir))


@needs_fred
def test_fetch_all_series(fred_handler: FREDAPIHandler) -> None:
    """Test fetching all series data."""
    results = fred_handler.fetch_all_series()
    for series_name in SERIES_MAPPING.values():
        assert series_name in results
        assert results[series_name] is True


@needs_fred
def test_transform_data(fred_handler: FREDAPIHandler) -> None:
    """Test transforming fetched data."""
    fred_handler.fetch_all_series()
    transformed_data = fred_handler.transformer.transform_data()
    assert transformed_data is not None
    assert isinstance(transformed_data, dict)


@needs_fred
def test_classify_data(fred_handler: FREDAPIHandler) -> None:
    """Test classifying transformed data."""
    transformed_data = {"date1": {"consumer_confidence": 100.0}}  # Change to float value
    classified_data = fred_handler.classifier.classify_data(transformed_data)
    assert classified_data is not None
    assert isinstance(classified_data, dict)
    for metrics in classified_data.values():
        for metric_data in metrics.values():
            assert isinstance(metric_data, dict)
            assert "value" in metric_data
            assert "category" in metric_data
            assert "description" in metric_data
            assert "impact" in metric_data
            assert "label" in metric_data


@needs_fred
def test_process_data(fred_handler: FREDAPIHandler) -> None:
    """Test processing data through the entire pipeline."""
    fred_data = fred_handler.process_data()
    assert fred_data is not None
    assert isinstance(fred_data, dict)

    # Check that the data contains the expected keys
    for month_data in fred_data.values():
        assert "consumer_confidence" in month_data
        assert "unemployment_rate" in month_data
        assert "inflation_rate" in month_data
        assert "gdp_growth_rate" in month_data
        assert "federal_funds_rate" in month_data
        assert "retail_sales" in month_data

        # Check the structure of each metric
        for metric in [
            "consumer_confidence",
            "unemployment_rate",
            "inflation_rate",
            "gdp_growth_rate",
            "federal_funds_rate",
            "retail_sales",
        ]:
            assert isinstance(month_data[metric], dict)
            assert "value" in month_data[metric]
            assert "category" in month_data[metric]
            assert "description" in month_data[metric]
            assert "impact" in month_data[metric]
            assert "label" in month_data[metric]
