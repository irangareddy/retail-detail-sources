"""Data classes for weather statistics."""

from dataclasses import dataclass, field


@dataclass
class WeatherStatistics:
    """Statistics for a weather metric."""

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
    """Monthly weather statistics."""

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
    """Weather data for a specific state."""

    state_name: str  # Name of the state (e.g., "California")
    monthly_weather: dict[int, MonthlyWeatherStats] = field(default_factory=dict)
