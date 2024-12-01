import json
import logging
import os
from typing import Any

from retail_data_sources.fred.classifier import FREDDataClassifier
from retail_data_sources.fred.constants import SERIES_MAPPING
from retail_data_sources.fred.fetcher import FREDDataFetcher
from retail_data_sources.fred.models.metric import FREDData
from retail_data_sources.fred.transformer import FREDTransformer

logger = logging.getLogger(__name__)


class FREDAPIHandler:
    def __init__(
        self,
        api_key: str = None,
        base_dir: str = "data",
        rules_file: str = None,
        rules_dict: dict = None,
    ):
        self.api_key = api_key or os.getenv("FRED_API_KEY")
        if not self.api_key:
            raise ValueError("FRED API key must be provided")

        self.base_dir = base_dir
        os.makedirs(self.base_dir, exist_ok=True)

        # Initialize components
        self.fetcher = FREDDataFetcher(self.api_key, self.base_dir)
        self.transformer = FREDTransformer(self.base_dir)
        self.classifier = FREDDataClassifier(rules_file, rules_dict)

    def fetch_all_series(self) -> dict[str, bool]:
        """Fetch all configured FRED series data."""
        results = {}
        for series_id in SERIES_MAPPING.keys():
            try:
                data = self.fetcher.fetch_series(series_id)
                results[SERIES_MAPPING[series_id]] = data is not None
            except Exception as e:
                logger.error(f"Error fetching {series_id}: {e}")
                results[SERIES_MAPPING[series_id]] = False
        return results

    def _cleanup_tmp_files(self):
        """Clean up temporary files after processing."""
        import shutil

        tmp_dir = os.path.join(self.base_dir, "tmp")
        if os.path.exists(tmp_dir):
            try:
                shutil.rmtree(tmp_dir)
                logger.info("Cleaned up temporary files")
            except Exception as e:
                logger.error(f"Error cleaning up temporary files: {e}")

    def process_data(self, fetch: bool = True) -> FREDData:
        """Process FRED data through the entire pipeline."""
        try:
            # Step 1: Fetch data if requested
            if fetch:
                fetch_results = self.fetch_all_series()
                if not any(fetch_results.values()):
                    logger.error("Failed to fetch any FRED series")
                    return None

            # Step 2: Transform data
            transformed_data = self.transformer.transform_data()
            if not transformed_data:
                logger.error("Data transformation failed")
                return None

            # Step 3: Classify data
            classified_data = self.classifier.classify_data(transformed_data)
            with open("../../samples/fred/classified_data.json", "w") as f:
                json.dump(classified_data, f, indent=2)
            if not classified_data:
                logger.error("Data classification failed")
                return None

            # Clean up temporary files after successful processing
            self._cleanup_tmp_files()
            fred_data = FREDData.from_dict(classified_data)
            return fred_data

        except Exception as e:
            logger.error(f"Error in data processing pipeline: {e}")
            # Attempt to clean up temporary files even if processing failed
            self._cleanup_tmp_files()
            return None

    def save_data(self, data: dict[str, Any], filepath: str) -> bool:
        """Save data to JSON file."""
        try:
            os.makedirs(os.path.dirname(filepath), exist_ok=True)
            with open(filepath, "w", encoding="utf-8") as f:
                json.dump(data, f, indent=2)
            logger.info(f"Successfully saved data to {filepath}")
            return True
        except Exception as e:
            logger.error(f"Error saving data to {filepath}: {e}")
            return False


def main():
    # Example usage
    handler = FREDAPIHandler(api_key=None)
    fred_data = handler.process_data(fetch=True)
    if fred_data:
        print(fred_data)


if __name__ == "__main__":
    main()
