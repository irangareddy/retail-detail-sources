"""FRED API handler to fetch, transform, and classify economic data."""

import logging
import os
from pathlib import Path

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
        base_dir: str = "tmp/fred",
        rules_dict: dict | None = None,
    ):
        self.api_key = api_key or os.getenv("FRED_API_KEY")
        if not self.api_key:
            raise ValueError("FRED API key must be provided")

        self.base_dir = base_dir
        Path.mkdir(Path(self.base_dir), exist_ok=True)

        # Initialize components
        self.fetcher = FREDDataFetcher(self.api_key, self.base_dir)
        self.transformer = FREDTransformer(self.base_dir)
        self.classifier = FREDDataClassifier(rules_dict)

    def fetch_all_series(self) -> dict[str, bool]:
        """Fetch all configured FRED series data."""
        results = {}
        for series_id in SERIES_MAPPING:
            try:
                data = self.fetcher.fetch_series(series_id)
                results[SERIES_MAPPING[series_id]] = data is not None
            except Exception:
                logger.exception(f"Error fetching {series_id}")
                results[SERIES_MAPPING[series_id]] = False
        return results

    def _cleanup_tmp_files(self) -> None:
        """Clean up temporary files after processing."""
        import shutil

        tmp_dir = Path(self.base_dir, "tmp")
        if Path.exists(tmp_dir):
            try:
                shutil.rmtree(tmp_dir)
                logger.info("Cleaned up temporary files")
            except Exception:
                logger.exception("Error cleaning up temporary files")

    def process_data(self, fetch: bool = True) -> dict:
        """Process FRED data through the entire pipeline and return JSON."""
        try:
            # Step 1: Fetch data if requested
            if fetch:
                fetch_results = self.fetch_all_series()
                if not any(fetch_results.values()):
                    logger.error("Failed to fetch any FRED series")
                    return {}

            # Step 2: Transform data
            transformed_data = self.transformer.transform_data()
            if not transformed_data:
                logger.error("Data transformation failed")
                return {}

            # Step 3: Classify data
            classified_data = self.classifier.classify_data(transformed_data)

            # Clean up temporary files after successful processing
            self._cleanup_tmp_files()

        except Exception:
            logger.exception("Error in data processing pipeline")
            # Attempt to clean up temporary files even if processing failed
            self._cleanup_tmp_files()
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
