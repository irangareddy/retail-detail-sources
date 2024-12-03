"""Testing utilities."""

import os

import pytest

needs_fred = pytest.mark.skipif(
    os.environ.get("FRED_API_KEY", "") == "", reason="Needs FRED API KEY"
)

needs_census = pytest.mark.skipif(
    os.environ.get("CENSUS_API_KEY", "") == "", reason="Needs Census API KEY"
)

needs_open_weather = pytest.mark.skipif(
    os.environ.get("OPEN_WEATHER_API_KEY", "") == "", reason="Needs Open Weather API KEY"
)
