"""census.models.retail_sales"""

from dataclasses import dataclass


@dataclass
class SalesData:
    """Sales data for a specific NAICS code."""

    sales_value: float
    state_share: float


@dataclass
class StateData:
    """Sales data for a specific state."""

    state_code: str
    sales: dict[str, SalesData]


@dataclass
class NationalTotal:
    """National total sales data."""

    sales_445: float
    sales_448: float


@dataclass
class RetailResponse:
    """Retail sales data response."""

    date: str
    states: dict[str, StateData]
    national_total: NationalTotal


@dataclass
class ValidationMetrics:
    """Validation metrics for retail sales data."""

    completeness: float
    consistency: float
    timestamp: str
