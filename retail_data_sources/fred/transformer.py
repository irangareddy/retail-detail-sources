"""Transform FRED data into unified format."""

import json
import logging
from pathlib import Path
from typing import Any

from utils.constants import SERIES_MAPPING

logger = logging.getLogger(__name__)


class FREDTransformer:
    """Transform FRED data into unified format."""

    def __init__(self, input_dir: str = "data/fred"):
        self.input_dir = input_dir

    def get_latest_files(self) -> dict[str, Path]:
        """Get the latest temporary file for each series."""
        latest_files = {}
        tmp_dir = Path(self.input_dir, "tmp")
        for series_id in SERIES_MAPPING.keys():
            # Look for temporary files for this series
            files = list(Path(tmp_dir).glob(f"tmp_{SERIES_MAPPING[series_id]}_*.json"))
            if files:
                latest_files[series_id] = max(files)
        return latest_files

    def extract_data_points(self, data: dict[str, Any]) -> dict[str, float]:
        """Extract date-value pairs from FRED data."""
        result = {}
        if "observations" in data:
            for obs in data["observations"]:
                date = obs["date"][:7]  # Convert YYYY-MM-DD to YYYY-MM
                try:
                    value = float(obs["value"]) if obs["value"] not in ["", "."] else None
                    result[date] = value
                except (ValueError, TypeError):
                    result[date] = None
        return result

    def transform_data(self) -> dict[str, dict[str, Any]]:
        """Transform FRED data into unified format."""
        latest_files = self.get_latest_files()
        series_data = {}

        for series_id, filepath in latest_files.items():
            try:
                with Path.open(filepath, encoding="utf-8") as f:
                    data = json.load(f)
                series_data[series_id] = self.extract_data_points(data)
            except Exception as e:
                logger.error(f"Error processing file {filepath}: {e}")
                series_data[series_id] = {}

        # Combine all series data
        all_dates = {date for series in series_data.values() for date in series.keys()}
        return {
            date: {
                SERIES_MAPPING[series_id]: series_data[series_id].get(date)
                for series_id in SERIES_MAPPING.keys()
            }
            for date in sorted(all_dates)
        }
