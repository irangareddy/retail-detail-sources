import os
from typing import Dict, List, Optional
import requests
import json
from state_weather_data import WeatherStats, MonthlyWeatherData, StateWeatherData


class WeatherDataProcessor:
    def __init__(self, api_key: str):
        self.api_key = api_key
        self.base_url = "https://history.openweathermap.org/data/2.5/aggregated/month"
        self.us_states = {
            "CA": (36.7783, -119.4179),
            "NY": (42.1497, -74.9384),
            "TX": (31.9686, -99.9018),
            # Add more states here as needed
        }
        self.months = range(1, 13)  # Fetch data for all 12 months


    def fetch_and_parse_weather_data(self, lat: float, lon: float, month: int) -> Optional[MonthlyWeatherData]:
        url = f"{self.base_url}?lat={lat}&lon={lon}&month={month}&appid={self.api_key}"
        try:
            response = requests.get(url, timeout=30)  # Added timeout for better error handling
            response.raise_for_status()
            data = response.json()

            if data["cod"] != 200:
                print(f"OpenWeather API Error ({data['cod']}): {data.get('message', 'Unknown error')}")
                return None

            result = data.get("result", {})

            def safe_get_weather_stats(weather_dict: dict) -> WeatherStats:
                return WeatherStats(
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

            return MonthlyWeatherData(
                month=result.get("month", 0),
                temp=safe_get_weather_stats(result.get("temp", {})),
                pressure=safe_get_weather_stats(result.get("pressure", {})),
                humidity=safe_get_weather_stats(result.get("humidity", {})),
                wind=safe_get_weather_stats(result.get("wind", {})),
                precipitation=safe_get_weather_stats(result.get("precipitation", {})),
                clouds=safe_get_weather_stats(result.get("clouds", {})),
                sunshine_hours=result.get("sunshine_hours", 0.0),
            )

        except requests.exceptions.RequestException as e:
            print(f"Request Error: {e}")
            return None
        except (KeyError, json.JSONDecodeError) as e:
            print(f"Data Parsing Error: {e}")
            return None
        except Exception as e:
            print(f"An unexpected error occurred: {e}")
            return None


    def process_data(self, output_filename: str = "weather_data.json") -> None:
        all_states_data = {}
        for state, (lat, lon) in self.us_states.items():
            state_data = {}
            for month in self.months:
                monthly_data = self.fetch_and_parse_weather_data(lat, lon, month)
                if monthly_data:
                    state_data[str(monthly_data.month)] = monthly_data
            all_states_data[state] = state_data

            print(all_states_data)

        with open(output_filename, "w") as f:
            json.dump(all_states_data, f, indent=4, default=lambda o: o.__dict__)

        print(f"Weather data fetched and saved to {output_filename}")


def main():
    api_key = os.getenv("OPENWEATHER_API_KEY")
    if not api_key:
        raise ValueError("OPENWEATHER_API_KEY environment variable not set.")

    processor = WeatherDataProcessor(api_key)
    processor.process_data()


if __name__ == "__main__":
    main()