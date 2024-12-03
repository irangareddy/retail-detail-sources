"""census.models.retail_sales"""

from dataclasses import dataclass


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

    def to_dict(self) -> dict:
        """Convert entire report to dictionary format."""
        return {
            "metadata": {
                "last_updated": self.metadata.last_updated,
                "categories": self.metadata.categories,
            },
            "sales_data": {
                month: {
                    "states": {
                        state_code: state_data.to_dict()
                        for state_code, state_data in month_data.states.items()
                    },
                    "national_total": month_data.national_total.to_dict(),
                }
                for month, month_data in self.sales_data.items()
            },
        }
