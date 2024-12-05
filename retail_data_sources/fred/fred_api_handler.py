"""FRED API handler to fetch, transform, and classify economic data."""

import logging
import os
from typing import Any

from retail_data_sources.fred.classifier import FREDDataClassifier
from retail_data_sources.fred.fetcher import FREDDataFetcher
from retail_data_sources.fred.transformer import FREDTransformer
from retail_data_sources.utils.constants import SERIES_MAPPING

logger = logging.getLogger(__name__)


class FREDAPIHandler:
    """FRED API handler to fetch, transform, and classify economic data."""

    def __init__(
        self,
        api_key: str | None = None,
        rules_dict: dict | None = None,
    ):
        self.api_key = api_key or os.getenv("FRED_API_KEY")
        if not self.api_key:
            raise ValueError("FRED API key must be provided")

        # Initialize components
        self.fetcher = FREDDataFetcher(self.api_key)
        self.transformer = FREDTransformer()
        self.classifier = FREDDataClassifier(rules_dict)

    def fetch_all_series(self) -> dict[str, dict[str, Any]]:
        """Fetch all configured FRED series data."""
        data: dict[str, dict[str, Any]] = {}
        for series_id, series_name in SERIES_MAPPING.items():
            try:
                series_data = self.fetcher.fetch_series(series_id)
                data[series_name] = series_data if series_data is not None else {}
            except Exception:
                logger.exception(f"Error fetching {series_name}")
                data[series_name] = {}
        return data

    def process_data(self, fetch: bool = True) -> dict[str, dict[str, Any]]:
        """Process FRED data through the entire pipeline and return JSON."""
        try:
            # Step 1: Fetch data if requested
            if fetch:
                fetched_data = self.fetch_all_series()

            # Step 2: Transform data
            transformed_data = self.transformer.transform_data(fetched_data)

            # Step 3: Classify data
            classified_data = self.classifier.classify_data(transformed_data)

        except Exception:
            logger.exception("Error in data processing pipeline")
            return {}
        else:
            return classified_data


def main() -> None:
    """Usage of the FRED API handler."""
    # Example usage
    handler = FREDAPIHandler(api_key=None)
    economic_data = handler.process_data(fetch=True)
    if economic_data:
        logging.info(economic_data)


if __name__ == "__main__":
    main()
