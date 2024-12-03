import json
import os

import pytest

from retail_data_sources.fred.constants import SERIES_MAPPING
from retail_data_sources.fred.fred_api_handler import FREDAPIHandler
from retail_data_sources.fred.models.economic_metrics import FREDData, Metric


@pytest.fixture
def fred_handler(tmp_path):
    assert "FRED_API_KEY" in os.environ, "FRED_API_KEY must be set in the environment"
    handler = FREDAPIHandler(base_dir=str(tmp_path))
    handler.transformer.series_mapping = SERIES_MAPPING
    return handler


def test_initialization(fred_handler):
    assert fred_handler.api_key == os.environ["FRED_API_KEY"]
    assert os.path.exists(fred_handler.base_dir)


def test_fetch_all_series(fred_handler):
    results = fred_handler.fetch_all_series()
    for series_id, series_name in SERIES_MAPPING.items():
        assert series_name in results
        assert results[series_name] is True


def test_transform_data(fred_handler):
    fetch_results = fred_handler.fetch_all_series()
    transformed_data = fred_handler.transformer.transform_data()
    assert transformed_data is not None
    assert isinstance(transformed_data, dict)


def test_classify_data(fred_handler):
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


def test_process_data(fred_handler):
    fred_data = fred_handler.process_data()
    assert fred_data is not None
    assert isinstance(fred_data, FREDData)
    for metric in fred_data.metrics:
        assert isinstance(metric.consumer_confidence, Metric)
        assert isinstance(metric.unemployment_rate, Metric)
        assert isinstance(metric.inflation_rate, Metric)
        assert isinstance(metric.gdp_growth_rate, Metric)
        assert isinstance(metric.federal_funds_rate, Metric)
        assert isinstance(metric.retail_sales, Metric)


def test_save_data(tmp_path):
    test_data = {"test": "data"}
    output_file = tmp_path / "subdir" / "test.json"
    handler = FREDAPIHandler(base_dir=str(tmp_path))
    result = handler.save_data(test_data, str(output_file))
    assert result is True
    assert output_file.exists()
    with open(output_file) as f:
        saved_data = json.load(f)
    assert saved_data == test_data
