"""Transform FRED data into unified format."""

import logging
from typing import Any

logger = logging.getLogger(__name__)


class FREDTransformer:
    """Transform FRED data into unified format."""

    def transform_data(self, fetched_data: dict[str, dict[str, Any]]) -> dict[str, dict[str, Any]]:
        """Transform FRED data into unified format."""
        series_data: dict[str, dict[str, float]] = {}
        all_dates: set[str] = set()

        for series_name, data in fetched_data.items():
            try:
                series_data[series_name] = self.extract_data_points(data)
                all_dates.update(series_data[series_name].keys())
            except Exception:
                logger.exception(f"Error processing data for series {series_name}")
                series_data[series_name] = {}

        return {
            date: {series_name: series_data[series_name].get(date) for series_name in series_data}
            for date in sorted(all_dates)
        }

    def extract_data_points(self, data: dict[str, Any]) -> dict[str, float]:
        """Extract date-value pairs from FRED data."""
        result: dict[str, float] = {}
        if "observations" in data:
            for obs in data["observations"]:
                date = obs["date"][:7]  # Convert YYYY-MM-DD to YYYY-MM
                try:
                    value = float(obs["value"]) if obs["value"] not in ["", "."] else None
                    if value is not None:  # Only include non-None values
                        result[date] = value
                except (ValueError, TypeError):
                    pass
        return result
