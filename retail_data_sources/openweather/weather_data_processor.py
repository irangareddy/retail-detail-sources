import json
import os

import requests

from retail_data_sources.openweather.models.state_weather import (
    MonthlyWeatherStats,
    StateWeather,
    WeatherStatistics,
)


class WeatherDataProcessor:
    def __init__(self, api_key: str):
        self.api_key = api_key
        self.base_url = "https://history.openweathermap.org/data/2.5/aggregated/month"
        self.us_states = {
            "CA": (36.7783, -119.4179),
            # "NY": (42.1497, -74.9384),
            # "TX": (31.9686, -99.9018),
        }
        self.months = range(1, 3)

    def fetch_and_parse_weather_data(self, lat: float, lon: float, month: int) -> dict | None:
        url = f"{self.base_url}?lat={lat}&lon={lon}&month={month}&appid={self.api_key}"
        try:
            response = requests.get(url, timeout=30)
            response.raise_for_status()
            data = response.json()

            if data["cod"] != 200:
                print(f"OpenWeather API Error ({data['cod']}): {data.get('message', 'Unknown error')}")
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
            print(f"Request Error: {e}")
            return None
        except (KeyError, json.JSONDecodeError) as e:
            print(f"Data Parsing Error: {e}")
            return None
        except Exception as e:
            print(f"An unexpected error occurred: {e}")
            return None

    def process_data(self) -> list[StateWeather]:
        all_states_data = []
        for state, (lat, lon) in self.us_states.items():
            state_data = {}
            for month in self.months:
                monthly_data = self.fetch_and_parse_weather_data(lat, lon, month)
                if monthly_data:
                    state_data.update(monthly_data)
            all_states_data.append(StateWeather(state_name=state, monthly_weather=state_data))

        return all_states_data


def main():
    api_key = os.getenv("OPENWEATHER_API_KEY")
    if not api_key:
        raise ValueError("OPENWEATHER_API_KEY environment variable not set.")

    processor = WeatherDataProcessor(api_key)

    # Fetch and print processed data
    states_weather = processor.process_data()
    for state_weather in states_weather:
        print(state_weather)


if __name__ == "__main__":
    main()


StateWeather(state_name="CA", monthly_weather={"1": MonthlyWeatherStats(month=1, temp=WeatherStatistics(record_min=267.34, record_max=298.74, average_min=272.21, average_max=293.23, median=282.34, mean=282.49, p25=279.35, p75=285.43, st_dev=4.68, num=8928), pressure=WeatherStatistics(record_min=0.0, record_max=0.0, average_min=0.0, average_max=0.0, median=1021, mean=1020.21, p25=1017, p75=1024, st_dev=6.96, num=8928), humidity=WeatherStatistics(record_min=0.0, record_max=0.0, average_min=0.0, average_max=0.0, median=81, mean=78, p25=67, p75=93, st_dev=17.66, num=8870), wind=WeatherStatistics(record_min=0.0, record_max=0.0, average_min=0.0, average_max=0.0, median=1.9, mean=2.05, p25=1.25, p75=2.6, st_dev=1.48, num=8928), precipitation=WeatherStatistics(record_min=0.0, record_max=0.0, average_min=0.0, average_max=0.0, median=0, mean=0.05, p25=0, p75=0, st_dev=0.22, num=8928), clouds=WeatherStatistics(record_min=0.0, record_max=0.0, average_min=0.0, average_max=0.0, median=40, mean=42.49, p25=1, p75=90, st_dev=39.34, num=8928), sunshine_hours_total=89.42), "2": MonthlyWeatherStats(month=2, temp=WeatherStatistics(record_min=270.4, record_max=300.21, average_min=273, average_max=296.38, median=283.69, mean=284.03, p25=280.29, p75=287.52, st_dev=5.06, num=8136), pressure=WeatherStatistics(record_min=0.0, record_max=0.0, average_min=0.0, average_max=0.0, median=1019, mean=1019.11, p25=1016, p75=1023, st_dev=5.34, num=8136), humidity=WeatherStatistics(record_min=0.0, record_max=0.0, average_min=0.0, average_max=0.0, median=72, mean=69.48, p25=55, p75=86, st_dev=19.32, num=8122), wind=WeatherStatistics(record_min=0.0, record_max=0.0, average_min=0.0, average_max=0.0, median=2.06, mean=2.34, p25=1.34, p75=3.1, st_dev=2.58, num=8136), precipitation=WeatherStatistics(record_min=0.0, record_max=0.0, average_min=0.0, average_max=0.0, median=0, mean=0.05, p25=0, p75=0, st_dev=0.26, num=8136), clouds=WeatherStatistics(record_min=0.0, record_max=0.0, average_min=0.0, average_max=0.0, median=20, mean=33.6, p25=1, p75=75, st_dev=36.73, num=8136), sunshine_hours_total=114.17)})
