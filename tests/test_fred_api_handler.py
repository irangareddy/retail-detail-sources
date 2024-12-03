"""Test the FRED API handler."""

import json
import os
from pathlib import Path

import pytest
from utils.constants import SERIES_MAPPING

from retail_data_sources.fred.fred_api_handler import FREDAPIHandler
from retail_data_sources.fred.models.economic_metrics import EconomicData, EconomicMetric


@pytest.fixture
def fred_handler(tmp_path) -> FREDAPIHandler:
    """Create a FRED API handler for testing."""
    assert "FRED_API_KEY" in os.environ, "FRED_API_KEY must be set in the environment"
    handler = FREDAPIHandler(base_dir=str(tmp_path))
    handler.transformer.series_mapping = SERIES_MAPPING
    return handler


def test_initialization(fred_handler) -> None:
    """Test initializing the FRED API handler."""
    assert fred_handler.api_key == os.environ["FRED_API_KEY"]
    assert Path.exists(Path(fred_handler.base_dir))


def test_fetch_all_series(fred_handler) -> None:
    """Test fetching all series data."""
    results = fred_handler.fetch_all_series()
    for series_id, series_name in SERIES_MAPPING.items():
        assert series_name in results
        assert results[series_name] is True


def test_transform_data(fred_handler) -> None:
    """Test transforming fetched data."""
    fetch_results = fred_handler.fetch_all_series()
    transformed_data = fred_handler.transformer.transform_data()
    assert transformed_data is not None
    assert isinstance(transformed_data, dict)


def test_classify_data(fred_handler) -> None:
    """Test classifying transformed data."""
    transformed_data = {"date1": {"consumer_confidence": 100}}
    classified_data = fred_handler.classifier.classify_data(transformed_data)
    assert classified_data is not None
    assert isinstance(classified_data, dict)
    for date, metrics in classified_data.items():
        for metric_name, metric_data in metrics.items():
            assert isinstance(metric_data, dict)
            assert "value" in metric_data
            assert "category" in metric_data
            assert "description" in metric_data
            assert "impact" in metric_data
            assert "label" in metric_data


def test_process_data(fred_handler) -> None:
    """Test processing data through the entire pipeline."""
    fred_data = fred_handler.process_data()
    assert fred_data is not None
    assert isinstance(fred_data, EconomicData)
    for metric in fred_data.metrics:
        assert isinstance(metric.consumer_confidence, EconomicMetric)
        assert isinstance(metric.unemployment_rate, EconomicMetric)
        assert isinstance(metric.inflation_rate, EconomicMetric)
        assert isinstance(metric.gdp_growth_rate, EconomicMetric)
        assert isinstance(metric.federal_funds_rate, EconomicMetric)
        assert isinstance(metric.retail_sales, EconomicMetric)


def test_save_data(tmp_path) -> None:
    """Test saving data to a file."""
    test_data = {"test": "data"}
    output_file = tmp_path / "subdir" / "test.json"
    handler = FREDAPIHandler(base_dir=str(tmp_path))
    result = handler.save_data(test_data, str(output_file))
    assert result is True
    assert output_file.exists()
    with Path.open(output_file) as f:
        saved_data = json.load(f)
    assert saved_data == test_data
