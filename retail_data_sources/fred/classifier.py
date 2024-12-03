"""Classify FRED data based on interpretation rules."""

import json
import logging
import os
from pathlib import Path
from typing import Any

logger = logging.getLogger(__name__)


class FREDDataClassifier:
    """Classify FRED data based on interpretation rules."""

    def __init__(self, rules_file: str | None = None, rules_dict: dict | None = None):
        """Initialize the classifier with interpretation rules."""
        self.rules = rules_dict if rules_dict else self._load_rules(rules_file)

    def _load_rules(self, rules_file: str | None = None) -> dict[str, Any]:
        """Load interpretation rules from JSON file."""
        default_path = Path(__file__).parent / "fred_interpretation_rules.json"

        # Ensure rules_file is a string or path by converting None or environment variable
        rules_file = rules_file or os.getenv("FRED_RULES_FILE", str(default_path))

        # If rules_file is None after all, use the default path
        if rules_file is None:
            rules_file = str(default_path)

        # Ensure rules_file is a valid Path object
        path = Path(rules_file)

        try:
            # Open the file and load the JSON data
            with path.open() as f:
                data: dict[str, Any] = json.load(f)  # Explicitly type the data here

        except Exception:
            logger.exception("Error loading rules file")
            data = {}  # Return an empty dictionary in case of an error

        return data

    def get_threshold_category(self, metric: str, value: float) -> tuple[str, dict]:
        """Determine the threshold category for a given value."""
        rules = self.rules["metrics"][metric]["thresholds"]

        for category, info in rules.items():
            min_val, max_val = info["range"]
            min_val = float("-inf") if min_val is None else min_val
            max_val = float("inf") if max_val is None else max_val

            if min_val <= value <= max_val:
                return category, info

        return "undefined", {}

    def classify_value(self, metric: str, value: float) -> dict[str, Any]:
        """Classify a single value based on the rules."""
        try:
            if value is None:
                return {
                    "value": None,
                    "category": "unknown",
                    "description": "No data available",
                    "impact": "Unable to determine impact",
                    "label": self.rules["metrics"][metric]["label"],
                }

            category, info = self.get_threshold_category(metric, value)
            return {
                "value": value,
                "category": category,
                "description": info.get("description", ""),
                "impact": info.get("impact", ""),
                "label": self.rules["metrics"][metric]["label"],
            }
        except Exception:
            logger.exception(f"Error classifying {metric} value {value}")
            return {
                "value": value,
                "category": "error",
                "description": "Error in classification",
                "impact": "Unable to determine impact",
                "label": self.rules["metrics"][metric]["label"],
            }

    def classify_data(self, data: dict[str, dict[str, float]]) -> dict[str, dict[str, Any]]:
        """Classify all values in the FRED data."""
        return {
            date: {
                metric: self.classify_value(metric, value)
                for metric, value in metrics.items()
                if metric in self.rules["metrics"]
            }
            for date, metrics in data.items()
        }
