import json
import logging
import os
from datetime import datetime
from typing import Any

import requests

from .constants import SERIES_MAPPING

logger = logging.getLogger(__name__)


class FREDDataFetcher:
    def __init__(self, api_key: str, output_dir: str = "data/fred"):
        self.base_url = "https://api.stlouisfed.org/fred/series/observations"
        self.api_key = api_key
        self.output_dir = output_dir

    def build_url_params(self, series_id: str, start_date: str, end_date: str) -> dict[str, str]:
        """Build URL parameters for the API request."""
        return {
            "series_id": series_id,
            "api_key": self.api_key,
            "file_type": "json",
            "observation_start": start_date,
            "observation_end": end_date,
        }

    def fetch_series(
        self, series_id: str, start_date: str = None, end_date: str = None
    ) -> dict[str, Any] | None:
        """Fetch data for a single series from FRED API."""
        start_date = start_date or "2019-10-01"
        end_date = end_date or datetime.now().strftime("%Y-%m-%d")

        params = self.build_url_params(series_id, start_date, end_date)

        try:
            response = requests.get(self.base_url, params=params)
            response.raise_for_status()
            data = response.json()

            # Save the fetched data
            output_file = self._get_output_filename(series_id)
            self._save_to_json(data, output_file)

            return data
        except requests.RequestException as e:
            logger.error(f"Error fetching data for series {series_id}: {e}")
            return None

    def _get_output_filename(self, series_id: str) -> str:
        """Generate temporary output filename based on series ID."""
        base_name = SERIES_MAPPING.get(series_id, series_id.lower())
        # Create a temporary directory if it doesn't exist
        tmp_dir = os.path.join(self.output_dir, "tmp")
        os.makedirs(tmp_dir, exist_ok=True)
        # Use a timestamp to ensure uniqueness but prefix with tmp_
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        return os.path.join(tmp_dir, f"tmp_{base_name}_{timestamp}.json")

    def _save_to_json(self, data: dict[str, Any], output_file: str) -> bool:
        """Save the data to a JSON file."""
        try:
            os.makedirs(os.path.dirname(output_file), exist_ok=True)
            with open(output_file, "w", encoding="utf-8") as f:
                json.dump(data, f, indent=2)
            logger.info(f"Data successfully saved to {output_file}")
            return True
        except OSError as e:
            logger.error(f"Error saving data: {e}")
            return False
