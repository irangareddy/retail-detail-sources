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
        default_rules = {
            "metrics": {
                "consumer_confidence": {
                    "label": "Consumer Confidence Index (UMCSENT)",
                    "thresholds": {
                        "high_confidence": {
                            "range": [100, None],
                            "description": "Above 100 indicates positive consumer outlook",
                            "impact": "Increased consumer spending, higher demand in retail",
                        },
                        "low_confidence": {
                            "range": [0, 100],
                            "description": "Below 100 indicates low consumer confidence",
                            "impact": "Consumers are more cautious, reducing demand",
                        },
                    },
                },
                "unemployment_rate": {
                    "label": "Unemployment Rate (UNRATE)",
                    "thresholds": {
                        "low_unemployment": {
                            "range": [0, 5],
                            "description": "Below 5% suggests a strong economy",
                            "impact": "Increased demand for goods and services",
                        },
                        "moderate_unemployment": {
                            "range": [5, 10],
                            "description": "Between 5-10% indicates economic challenges",
                            "impact": "Moderate caution in consumer spending",
                        },
                        "high_unemployment": {
                            "range": [10, None],
                            "description": "Above 10% suggests a struggling economy",
                            "impact": "Lower demand for non-essential items",
                        },
                    },
                },
                "inflation_rate": {
                    "label": "Inflation Rate (CPIAUCSL)",
                    "thresholds": {
                        "low_inflation": {
                            "range": [0, 3],
                            "description": "Below 3% suggests a stable economy",
                            "impact": "Stable or growing demand across categories",
                        },
                        "moderate_inflation": {
                            "range": [3, 5],
                            "description": "3-5% indicates moderate inflation",
                            "impact": "Slight caution in consumer spending",
                        },
                        "high_inflation": {
                            "range": [5, None],
                            "description": "Above 5% indicates high inflation",
                            "impact": "Reduced purchasing power and demand",
                        },
                    },
                },
                "retail_sales": {
                    "label": "Retail Sales Index (RSXFS)",
                    "thresholds": {
                        "strong_growth": {
                            "range": [5, None],
                            "description": "Strong growth in retail sales",
                            "impact": "Robust consumer spending across categories",
                        },
                        "moderate_growth": {
                            "range": [0, 5],
                            "description": "Moderate growth in retail sales",
                            "impact": "Stable consumer spending",
                        },
                        "decline": {
                            "range": [None, 0],
                            "description": "Declining retail sales",
                            "impact": "Reduced consumer demand",
                        },
                    },
                },
                "gdp_growth_rate": {
                    "label": "GDP Growth Rate (A191RL1Q225SBEA)",
                    "thresholds": {
                        "strong_growth": {
                            "range": [2, None],
                            "description": "Strong positive growth suggests "
                            "robust economic expansion",
                            "impact": "Higher GDP growth signals rising demand for "
                            "consumer goods and services",
                        },
                        "moderate_growth": {
                            "range": [0, 2],
                            "description": "Moderate growth indicates stable economic conditions",
                            "impact": "Stable demand across most sectors with potential for growth",
                        },
                        "contraction": {
                            "range": [None, 0],
                            "description": "Negative growth suggests economic contraction",
                            "impact": "Decreased demand due to economic slowdown, "
                            "focus on essential products",
                        },
                    },
                },
                "federal_funds_rate": {
                    "label": "Federal Funds Rate (FEDFUNDS)",
                    "thresholds": {
                        "low_rate": {
                            "range": [0, 2],
                            "description": "Below 2% signals easy borrowing conditions",
                            "impact": "Stimulates consumer spending and investment, "
                            "increased demand in retail",
                        },
                        "moderate_rate": {
                            "range": [2, 5],
                            "description": "2-5% signals neutral economic conditions",
                            "impact": "Neutral conditions with manageable "
                            "borrowing costs, stable demand",
                        },
                        "high_rate": {
                            "range": [5, None],
                            "description": "Above 5% signals tight monetary policy",
                            "impact": "High borrowing costs may reduce "
                            "consumer demand for non-essentials",
                        },
                    },
                },
            }
        }

        rules_file = rules_file or os.getenv("FRED_RULES_FILE", str(default_path))
        if rules_file is None:
            rules_file = str(default_path)

        path = Path(rules_file)

        try:
            with path.open() as f:
                data: dict[str, Any] = json.load(f)
        except Exception:
            logger.exception("Error loading rules file")
            data = default_rules

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
