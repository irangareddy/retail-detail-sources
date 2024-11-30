"""Classify FRED data based on interpretation rules."""
import json
import logging
import os
from typing import Any

logger = logging.getLogger(__name__)


class FREDDataClassifier:
    """Classify FRED data based on interpretation rules."""
    def __init__(self, rules_file: str = None, rules_dict: dict = None):
        """Initialize the classifier with interpretation rules."""
        self.rules = rules_dict if rules_dict else self._load_rules(rules_file)

    def _load_rules(self, rules_file: str = None) -> dict:
        """Load interpretation rules from JSON file."""
        default_path = os.path.join(os.path.dirname(__file__), "fred_interpretation_rules.json")
        rules_file = rules_file or os.getenv("FRED_RULES_FILE", default_path)

        try:
            with open(rules_file) as f:
                return json.load(f)
        except Exception as e:
            logger.error(f"Error loading rules file: {e}")
            raise

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
        except Exception as e:
            logger.error(f"Error classifying {metric} value {value}: {e}")
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
