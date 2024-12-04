"""Retail sales data models for the Census Bureau retail report."""

import json
from dataclasses import dataclass
from typing import Any


@dataclass
class Sales:
    """Sales data for a specific category."""

    sales_value: float
    state_share: float

    def to_dict(self) -> dict[str, float]:
        """Convert to dictionary ensuring Python float types."""
        return {"sales_value": float(self.sales_value), "state_share": float(self.state_share)}


@dataclass
class StateData:
    """Sales data for a specific state."""

    category_445: Sales | None = None
    category_448: Sales | None = None

    def to_dict(self) -> dict[str, dict[str, float] | None]:
        """Convert to dictionary with category codes as keys."""
        return {
            "445": None if self.category_445 is None else self.category_445.to_dict(),
            "448": None if self.category_448 is None else self.category_448.to_dict(),
        }


@dataclass
class CategoryTotal:
    """Total sales for a category."""

    category_445: float
    category_448: float

    def to_dict(self) -> dict[str, float]:
        """Convert to dictionary ensuring Python float types."""
        return {"445": float(self.category_445), "448": float(self.category_448)}


@dataclass
class MonthData:
    """Data for a specific month."""

    states: dict[str, StateData]  # state_code -> StateData
    national_total: CategoryTotal


@dataclass
class Metadata:
    """Metadata for the retail report."""

    last_updated: str
    categories: dict[str, str]


@dataclass
class RetailReport:
    """Complete retail sales report."""

    metadata: Metadata
    sales_data: dict[str, MonthData]  # month -> MonthData

    def __getitem__(self, key: str) -> dict[str, Any]:
        """Make RetailReport subscriptable."""
        if isinstance(self.metadata, dict):
            return self.metadata
        if key == "metadata":
            return {
                "last_updated": self.metadata.last_updated,
                "categories": self.metadata.categories,
            }
        if key == "sales_data":
            return {
                month: {
                    "states": {
                        state_code: state_data.to_dict()
                        for state_code, state_data in month_data.states.items()
                    },
                    "national_total": month_data.national_total.to_dict(),
                }
                for month, month_data in self.sales_data.items()
            }
        raise KeyError(f"Invalid key: {key}")

    def to_dict(self) -> dict:
        """Convert entire report to dictionary format."""
        # Ensure that metadata and sales_data are properly converted to dict
        metadata = self["metadata"]
        sales_data = self["sales_data"]
        return {
            "metadata": metadata,
            "sales_data": sales_data,
        }

    def to_json(self) -> str:
        """Convert entire report to JSON string."""
        return json.dumps(self.to_dict(), indent=4)

    @classmethod
    def from_dict(cls, data: dict) -> "RetailReport":
        """Convert a dictionary to a RetailReport instance."""
        metadata = Metadata(**data["metadata"])
        sales_data = {
            month: MonthData(
                states={
                    state_code: StateData(**state_data)
                    for state_code, state_data in month_data["states"].items()
                },
                national_total=CategoryTotal(**month_data["national_total"]),
            )
            for month, month_data in data["sales_data"].items()
        }
        return cls(metadata=metadata, sales_data=sales_data)

    @classmethod
    def from_json(cls, json_data: str) -> "RetailReport":
        """Convert a JSON string to a RetailReport instance."""
        return cls.from_dict(json.loads(json_data))
