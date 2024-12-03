from dataclasses import dataclass, field


@dataclass
class WeatherStatistics:
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
class MonthlyWeatherStats:
    month: int
    temp: WeatherStatistics
    pressure: WeatherStatistics
    humidity: WeatherStatistics
    wind: WeatherStatistics
    precipitation: WeatherStatistics
    clouds: WeatherStatistics
    sunshine_hours_total: float

@dataclass
class StateWeather:
    state_name: str  # Name of the state (e.g., "California")
    monthly_weather: dict[int, MonthlyWeatherStats] = field(default_factory=dict)
