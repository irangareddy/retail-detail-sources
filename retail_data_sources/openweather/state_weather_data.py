from dataclasses import dataclass, field
from typing import Dict, Any, Optional

import json
from dataclasses import dataclass
from typing import Dict, Any

@dataclass
class WeatherStats:
    record_min: float
    record_max: float
    average_min: float
    average_max: float
    median: float
    mean: float
    p25: float
    p75: float
    st_dev: float
    num: int

@dataclass
class MonthlyWeatherData:
    month: int
    temp: WeatherStats
    pressure: WeatherStats
    humidity: WeatherStats
    wind: WeatherStats
    precipitation: WeatherStats
    clouds: WeatherStats
    sunshine_hours: float

@dataclass
class StateWeatherData:
    weather_data: Dict[str, MonthlyWeatherData] = field(default_factory=dict) #Added default factory


def parse_weather_data(weather_data_json: str) -> Optional[Dict[str, StateWeatherData]]:
    try:
        weather_data = json.loads(weather_data_json)
        parsed_data = {}

        for state, state_data in weather_data.items():
            state_weather_data = {}
            for month, month_data in state_data.items():
              #handle missing sunshine_hours gracefully
                sunshine_hours = month_data.get("sunshine_hours", 0.0) #added default 0.0

                #Improved handling of missing WeatherStats data
                temp_stats = month_data.get("temp", {})
                pressure_stats = month_data.get("pressure", {})
                humidity_stats = month_data.get("humidity", {})
                wind_stats = month_data.get("wind", {})
                precipitation_stats = month_data.get("precipitation", {})
                clouds_stats = month_data.get("clouds", {})

                #added default values
                default_weather_stats = {'record_min': 0.0, 'record_max': 0.0, 'average_min': 0.0, 'average_max': 0.0, 'median': 0.0, 'mean': 0.0, 'p25': 0.0, 'p75': 0.0, 'st_dev': 0.0, 'num': 0}
                temp_stats = {**default_weather_stats,**temp_stats}
                pressure_stats = {**default_weather_stats,**pressure_stats}
                humidity_stats = {**default_weather_stats,**humidity_stats}
                wind_stats = {**default_weather_stats,**wind_stats}
                precipitation_stats = {**default_weather_stats,**precipitation_stats}
                clouds_stats = {**default_weather_stats,**clouds_stats}


                state_weather_data[month] = MonthlyWeatherData(
                    month=month_data["month"],
                    temp=WeatherStats(**temp_stats),
                    pressure=WeatherStats(**pressure_stats),
                    humidity=WeatherStats(**humidity_stats),
                    wind=WeatherStats(**wind_stats),
                    precipitation=WeatherStats(**precipitation_stats),
                    clouds=WeatherStats(**clouds_stats),
                    sunshine_hours=sunshine_hours
                )

            parsed_data[state] = StateWeatherData(weather_data=state_weather_data)
        return parsed_data
    except (json.JSONDecodeError, KeyError) as e:
        print(f"Error parsing weather data: {e}")
        return None
    except Exception as e:
        print(f"An unexpected error occurred: {e}")
        return None