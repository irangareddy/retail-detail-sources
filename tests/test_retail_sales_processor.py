"""Tests for the RetailSalesProcessor class using the real Census API."""

import os

import pytest

from retail_data_sources.census.retail_sales_processor import RetailSalesProcessor
from tests.utils import needs_census


@pytest.fixture
@needs_census
def processor() -> RetailSalesProcessor:
    """Fixture to create a RetailSalesProcessor instance for testing with real API key."""
    api_key = os.getenv("CENSUS_API_KEY")
    return RetailSalesProcessor(api_key=api_key)


# Test initialization of the RetailSalesProcessor
def test_initialization(processor: RetailSalesProcessor) -> None:
    """Test initializing the RetailSalesProcessor with the real Census API key."""
    assert processor.api_key == os.getenv("CENSUS_API_KEY")
    assert processor.categories == {
        "445": "Food and Beverage Stores",
        "448": "Clothing and Accessories Stores",
    }
    assert processor.logger is not None


# Test for fetching MARTS data from the real API
def test_fetch_marts_data(processor: RetailSalesProcessor) -> None:
    """Test fetching MARTS data from the real Census API."""
    try:
        # Fetch data for the year 2024 and category "445" (Food and Beverage Stores)
        marts_data = processor.fetch_marts_data(year="2024", category="445")

        # Check if data is returned and that it's not empty
        assert not marts_data.empty, "MARTS data should not be empty"
        assert "cell_value" in marts_data.columns, "Expected column 'cell_value' in MARTS data"

    except ValueError:
        pytest.fail("Failed to fetch MARTS data")


# Test for fetching CBP data from the real API
def test_fetch_cbp_data(processor: RetailSalesProcessor) -> None:
    """Test fetching CBP data from the real Census API."""
    try:
        # Fetch CBP data for category "445" (Food and Beverage Stores)
        cbp_data = processor.fetch_cbp_data(category="445")

        # Check if data is returned and validate its structure
        assert isinstance(cbp_data, dict), "CBP data should be returned as a dictionary"
        assert "01" in cbp_data, "State '01' (AL) should be present in the CBP data"
        assert "weight" in cbp_data["01"], "State '01' should have a weight field in CBP data"

    except ValueError:
        pytest.fail("Failed to fetch CBP data")


# Test processing the real data through the pipeline
def test_process_data(processor: RetailSalesProcessor) -> None:
    """Test processing data through the entire RetailSalesProcessor pipeline."""
    try:
        # Process data for the year 2024
        data = processor.process_data(years=["2024"])

        # Check that the processed data contains the expected structure
        assert "metadata" in data, "Processed data should contain 'metadata'"
        assert "sales_data" in data, "Processed data should contain 'sales_data'"
        assert isinstance(data["sales_data"], dict), "Sales data should be a dictionary"

        # Check if we have data for the expected month (e.g., January 2024)
        assert "2024-01-01" in data["sales_data"], "Data for January 2024 should be present"
        assert (
            "states" in data["sales_data"]["2024-01-01"]
        ), "States data should be present in January 2024"

        # Check if national totals are included
        assert (
            "national_total" in data["sales_data"]["2024-01-01"]
        ), "National total data should be present"

    except ValueError:
        pytest.fail("Failed to process data")


# Test validating the processed data
def test_validate_data(processor: RetailSalesProcessor) -> None:
    """Test validating the processed data."""
    try:
        # Get some real data (or use mock data here if needed)
        data = processor.process_data(years=["2024"])

        # Validate the data (checking for completeness and consistency)
        is_valid, validation_results = processor.validate_data(data)

        # Check if the validation passed
        assert is_valid is True, f"Validation failed: {validation_results}"
        assert isinstance(validation_results, dict), "Validation results should be a dictionary"

        # Check some sample validation for a given month
        month_validation = validation_results.get("2024-01-01", {})
        assert (
            "states_complete" in month_validation
        ), "Month validation should include 'states_complete'"
        assert month_validation["states_complete"] is True, "States should be complete"

    except ValueError:
        pytest.fail("Failed to validate data")
