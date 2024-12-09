"""Fetch data from the FRED API."""

import json
import logging
from datetime import datetime
from pathlib import Path
from typing import Any

import requests

from retail_data_sources.utils.constants import EASTERN, SERIES_MAPPING

logger = logging.getLogger(__name__)


class FREDDataFetcher:
    """Fetch data from the FRED API."""

    def __init__(self, api_key: str, output_dir: str | None = None):
        """Initialize the FREDDataFetcher."""
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
        self, series_id: str, start_date: str | None = None, end_date: str | None = None
    ) -> dict[str, Any] | None:
        """Fetch data for a single series from FRED API."""
        start_date = start_date or "2019-10-01"
        end_date = end_date or datetime.now(EASTERN).strftime("%Y-%m-%d")

        params = self.build_url_params(series_id, start_date, end_date)

        try:
            response = requests.get(self.base_url, params=params, timeout=10)
            response.raise_for_status()
            data: dict[str, Any] = response.json()

            # If output_dir is provided, save the fetched data
            if self.output_dir:
                output_file = self._get_output_filename(series_id)
                self._save_to_json(data, output_file)
            else:
                logger.info(
                    f"Data for {series_id} fetched successfully, "
                    f"but not saved (output_dir is None)."
                )

        except requests.RequestException:
            logger.exception(f"Error fetching data for series {series_id}")
            return None
        else:
            return data

    def _get_output_filename(self, series_id: str) -> str:
        """Generate output filename based on series ID."""
        base_name = SERIES_MAPPING.get(series_id, series_id.lower())

        # If output_dir is provided, use it, else return a dummy filename for logging
        if self.output_dir:
            tmp_dir = Path(self.output_dir, "tmp")
            tmp_dir.mkdir(exist_ok=True)
            timestamp = datetime.now(EASTERN).strftime("%Y%m%d_%H%M%S")
            return str(tmp_dir / f"tmp_{base_name}_{timestamp}.json")
        return f"tmp_{base_name}_no_output_dir.json"

    def _save_to_json(self, data: dict[str, Any], output_file: str) -> bool:
        """Save the data to a JSON file."""
        try:
            Path.mkdir(Path(output_file).parent, exist_ok=True)
            with Path.open(Path(output_file), "w", encoding="utf-8") as f:
                json.dump(data, f, indent=2)
            logger.info(f"Data successfully saved to {output_file}")

        except Exception:
            logger.exception("Error saving data")
            return False
        else:
            return True
