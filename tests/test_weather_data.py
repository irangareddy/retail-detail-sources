"""Tests for the weather data processor."""

import os

import pytest

from retail_data_sources.openweather.weather_data_processor import WeatherDataProcessor
from tests.utils import needs_open_weather

# You should ensure your `OPEN_WEATHER_API_KEY` is available in the environment.
# Alternatively, you could set it in the test setup if you're using a test API key.


@needs_open_weather
@pytest.fixture
def processor() -> WeatherDataProcessor:
    """Create a weather data processor for testing."""
    api_key = os.getenv("OPEN_WEATHER_API_KEY")
    if not api_key:
        raise ValueError("OPEN_WEATHER_API_KEY environment variable is not set.")
    return WeatherDataProcessor(api_key)


@needs_open_weather
def test_fetch_and_parse_weather_data(processor: WeatherDataProcessor) -> None:
    """Test the successful case of calling the OpenWeather API and parsing the data."""
    # Assuming the processor.fetch_and_parse_weather_data() returns valid data when called
    lat, lon = 36.7783, -119.4179  # Example for California
    month = 1  # January

    data = processor.fetch_and_parse_weather_data(lat, lon, month)

    assert data is not None, "Data should not be None"
    assert isinstance(data, dict), "Data should be of type dictionary"

    # Check if month key exists
    assert str(month) in data, "The month should be in the data"

    # Check if required fields exist in the response (these should exist based on the API response)
    monthly_data = data[str(month)]
    assert "temp" in monthly_data, "'temp' should be in the monthly data"
    assert "pressure" in monthly_data, "'pressure' should be in the monthly data"
    assert "humidity" in monthly_data, "'humidity' should be in the monthly data"
    assert "wind" in monthly_data, "'wind' should be in the monthly data"
    assert "precipitation" in monthly_data, "'precipitation' should be in the monthly data"
    assert "clouds" in monthly_data, "'clouds' should be in the monthly data"
    assert (
        "sunshine_hours_total" in monthly_data
    ), "'sunshine_hours_total' should be in the monthly data"


@needs_open_weather
def test_process_data(processor: WeatherDataProcessor) -> None:
    """Test the full data processing for all states."""
    # Ensure it returns data for all states listed in the processor's `us_states`
    all_states_data = processor.process_data()

    assert isinstance(all_states_data, list), "Processed data should be a list"

    # Check if the data includes the expected states (e.g., "CA" for California)
    state_names = [state["state_name"] for state in all_states_data]
    assert "CA" in state_names, "'CA' should be in the processed data"

    # Ensure that monthly weather data exists for each state
    for state_data in all_states_data:
        assert "monthly_weather" in state_data, "'monthly_weather' should exist in state data"
        assert isinstance(
            state_data["monthly_weather"], dict
        ), "'monthly_weather' should be a dictionary"
        for monthly_data in state_data["monthly_weather"].values():
            assert "temp" in monthly_data, "'temp' should be in monthly weather data"
            assert "pressure" in monthly_data, "'pressure' should be in monthly weather data"
            assert "humidity" in monthly_data, "'humidity' should be in monthly weather data"
