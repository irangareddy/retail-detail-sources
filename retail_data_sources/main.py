"""Retail data sources main module."""

import os
import json
from datetime import datetime

from retail_data_sources.fred.fred_api_handler import FREDAPIHandler
from retail_data_sources.census.retail_sales_processor import RetailSalesProcessor
from retail_data_sources.openweather.weather_data_processor import WeatherDataProcessor

class DataFetcher:
    def __init__(self):
        self.fred_api_key = os.getenv("FRED_API_KEY")
        self.census_api_key = os.getenv("CENSUS_API_KEY")
        self.openweather_api_key = os.getenv("OPENWEATHER_API_KEY")

        if not self.fred_api_key:
            raise ValueError("FRED API key not found in environment variables")
        if not self.census_api_key:
            raise ValueError("Census API key not found in environment variables")
        if not self.openweather_api_key:
            raise ValueError("OpenWeather API key not found in environment variables")

        self.fred_handler = FREDAPIHandler(api_key=self.fred_api_key)
        self.census_processor = RetailSalesProcessor(api_key=self.census_api_key)
        self.weather_processor = WeatherDataProcessor(api_key=self.openweather_api_key)

    def fetch_fred_data(self):
        fred_data = self.fred_handler.process_data(fetch=True)
        if fred_data:
            print("FRED Data:")
            print(type(fred_data))
        else:
            print("Failed to fetch FRED data")

    def fetch_census_data(self):
        current_year = datetime.now().year
        census_data = self.census_processor.process_retail_data([str(current_year)])
        if census_data:
            print("Census Data:")
            print(type(census_data))
        else:
            print("Failed to fetch Census data")

    def fetch_weather_data(self):
        weather_data = self.weather_processor.process_data()
        if weather_data:
            print("Weather Data:")
            print(type(weather_data))
        else:
            print("Failed to fetch Weather data")

    def fetch_all_data(self):
        self.fetch_fred_data()
        self.fetch_census_data()
        self.fetch_weather_data()

if __name__ == "__main__":
    fetcher = DataFetcher()
    fetcher.fetch_all_data()