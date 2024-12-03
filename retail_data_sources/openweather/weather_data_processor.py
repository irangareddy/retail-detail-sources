"""Fetch and process weather data from the OpenWeather API."""

import json
import logging
import os
from json import JSONDecodeError

import requests

from retail_data_sources.openweather.models.state_weather import (
    MonthlyWeatherStats,
    StateWeather,
    WeatherStatistics,
)

logger = logging.getLogger(__name__)


class WeatherDataProcessor:
    """Fetch and process weather data from the OpenWeather API."""

    def __init__(self, api_key: str):
        self.api_key = api_key
        self.base_url = "https://history.openweathermap.org/data/2.5/aggregated/month"
        self.us_states = {
            "CA": (36.7783, -119.4179),
            "NY": (42.1497, -74.9384),
            "TX": (31.9686, -99.9018),
        }
        self.months = range(1, 3)

    def fetch_and_parse_weather_data(self, lat: float, lon: float, month: int) -> dict | None:
        """Fetch and parse weather data from the OpenWeather API."""
        url = f"{self.base_url}?lat={lat}&lon={lon}&month={month}&appid={self.api_key}"
        try:
            response = requests.get(url, timeout=30)
            response.raise_for_status()
            data = response.json()

            if data["cod"] != requests.status_codes.codes.OK:
                logger.info(
                    f"OpenWeather API Error ({data['cod']}): {data.get('message', 'Unknown error')}"
                )
                return None

            result = data.get("result", {})

            def safe_get_weather_stats(weather_dict: dict) -> WeatherStatistics:
                return WeatherStatistics(
                    record_min=weather_dict.get("record_min", 0.0),
                    record_max=weather_dict.get("record_max", 0.0),
                    average_min=weather_dict.get("average_min", 0.0),
                    average_max=weather_dict.get("average_max", 0.0),
                    median=weather_dict.get("median", 0.0),
                    mean=weather_dict.get("mean", 0.0),
                    p25=weather_dict.get("p25", 0.0),
                    p75=weather_dict.get("p75", 0.0),
                    st_dev=weather_dict.get("st_dev", 0.0),
                    num=weather_dict.get("num", 0),
                )

            # Create MonthlyWeatherStats instance
            monthly_data = MonthlyWeatherStats(
                month=result.get("month", 0),
                temp=safe_get_weather_stats(result.get("temp", {})),
                pressure=safe_get_weather_stats(result.get("pressure", {})),
                humidity=safe_get_weather_stats(result.get("humidity", {})),
                wind=safe_get_weather_stats(result.get("wind", {})),
                precipitation=safe_get_weather_stats(result.get("precipitation", {})),
                clouds=safe_get_weather_stats(result.get("clouds", {})),
                sunshine_hours_total=result.get("sunshine_hours", 0.0),
            )

            # Return a dictionary containing the monthly data
            return {str(monthly_data.month): monthly_data}

        except requests.exceptions.RequestException as e:
            logger.info(f"Request Error: {e}")
            return None
        except (KeyError, json.JSONDecodeError) as e:
            logger.info(f"Data Parsing Error: {e}")
            return None
        except JSONDecodeError as e:
            logger.info(f"An unexpected error occurred: {e}")
            return None

    def process_data(self) -> list[StateWeather]:
        """Fetch and process weather data for all states."""
        all_states_data = []
        for state, (lat, lon) in self.us_states.items():
            state_data = {}
            for month in self.months:
                monthly_data = self.fetch_and_parse_weather_data(lat, lon, month)
                if monthly_data:
                    state_data.update(monthly_data)
            all_states_data.append(StateWeather(state_name=state, monthly_weather=state_data))

        return all_states_data


def main() -> None:
    """Fetch and process weather data from the OpenWeather API."""
    api_key = os.getenv("OPENWEATHER_API_KEY")
    if not api_key:
        raise ValueError("OPENWEATHER_API_KEY environment variable not set.")

    processor = WeatherDataProcessor(api_key)

    # Fetch and logger.info( processed data
    states_weather = processor.process_data()
    for state_weather in states_weather:
        logger.info(state_weather)


if __name__ == "__main__":
    main()
