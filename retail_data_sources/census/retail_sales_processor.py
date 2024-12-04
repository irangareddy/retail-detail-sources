"""Retail sales data processor using Census API."""

import json
import os
from datetime import datetime
from pathlib import Path
from typing import ClassVar

import pandas as pd
import requests

from retail_data_sources.census.models.retail_sales import (
    CategoryTotal,
    Metadata,
    MonthData,
    RetailReport,
    Sales,
    StateData,
)
from retail_data_sources.utils.constants import EASTERN
from retail_data_sources.utils.logging import setup_logging

LOW_DEVIATION: float = 0.98
HIGH_DEVIATION: float = 1.02
DATE_FORMAT_LENGTH: int = 6


class RetailSalesProcessor:
    """Retail sales data processor using Census API."""

    state_id_to_abbreviation: ClassVar[dict[str, str]] = {
        "01": "AL",
        "02": "AK",
        "04": "AZ",
        "05": "AR",
        "06": "CA",
        "08": "CO",
        "09": "CT",
        "10": "DE",
        "11": "DC",
        "12": "FL",
        "13": "GA",
        "15": "HI",
        "16": "ID",
        "17": "IL",
        "18": "IN",
        "19": "IA",
        "20": "KS",
        "21": "KY",
        "22": "LA",
        "23": "ME",
        "24": "MD",
        "25": "MA",
        "26": "MI",
        "27": "MN",
        "28": "MS",
        "29": "MO",
        "30": "MT",
        "31": "NE",
        "32": "NV",
        "33": "NH",
        "34": "NJ",
        "35": "NM",
        "36": "NY",
        "37": "NC",
        "38": "ND",
        "39": "OH",
        "40": "OK",
        "41": "OR",
        "42": "PA",
        "44": "RI",
        "45": "SC",
        "46": "SD",
        "47": "TN",
        "48": "TX",
        "49": "UT",
        "50": "VT",
        "51": "VA",
        "53": "WA",
        "54": "WV",
        "55": "WI",
        "56": "WY",
    }

    def __init__(self, api_key: str):
        self.api_key = api_key
        self.categories = {
            "445": "Food and Beverage Stores",
            "448": "Clothing and Accessories Stores",
        }
        self.logger = setup_logging()

    def fetch_marts_data(self, year: str, category: str) -> pd.DataFrame:
        """Fetch MARTS data for a specific year and category."""
        try:
            url = "https://api.census.gov/data/timeseries/eits/marts"
            params = {
                "get": "data_type_code,seasonally_adj,category_code,cell_value,error_data",
                "time": year,
                "category_code": category,
                "key": self.api_key,
            }

            response = requests.get(url, params=params, timeout=10)
            response.raise_for_status()
            data = response.json()

            df = pd.DataFrame(data[1:], columns=data[0])
            sales_df = df[(df["data_type_code"] == "SM") & (df["seasonally_adj"] == "no")].copy()
            sales_df["cell_value"] = pd.to_numeric(sales_df["cell_value"])
        except Exception:
            self.logger.exception(f"Error fetching MARTS data for {year}, category {category}")
            return pd.DataFrame()
        else:
            return sales_df

    def fetch_cbp_data(self, category: str) -> dict[str, dict[str, float]]:
        """Fetch and process CBP data for state weights."""
        try:
            url = "https://api.census.gov/data/2021/cbp"
            params = {
                "get": "GEO_ID,NAICS2017,ESTAB,PAYANN",
                "for": "state:*",
                "NAICS2017": category,
                "key": self.api_key,
            }

            response = requests.get(url, params=params, timeout=10)
            response.raise_for_status()
            data = response.json()

            df = pd.DataFrame(data[1:], columns=data[0])
            df["ESTAB"] = pd.to_numeric(df["ESTAB"])
            df["PAYANN"] = pd.to_numeric(df["PAYANN"])

            total_payroll = df["PAYANN"].sum()
            total_estab = df["ESTAB"].sum()

            state_weights = {}
            for _, row in df.iterrows():
                state_code = row["state"]
                if state_code in ["60", "66", "69", "72", "78"]:
                    continue

                payroll_weight = row["PAYANN"] / total_payroll
                estab_weight = row["ESTAB"] / total_estab
                final_weight = (payroll_weight * 0.6) + (estab_weight * 0.4)

                state_weights[state_code] = {
                    "weight": round(final_weight, 4),
                    "establishments": int(row["ESTAB"]),
                    "annual_payroll": int(row["PAYANN"]),
                }
        except Exception:
            self.logger.exception(f"Error fetching CBP data for category {category}")
            return {}
        else:
            return state_weights

    def calculate_state_sales(
        self, national_sales: float, state_weights: dict[str, dict[str, float]]
    ) -> dict[str, float]:
        """Calculate state-level sales using weights."""
        state_sales = {}
        for state_code, state_data in state_weights.items():
            state_sales[state_code] = round(national_sales * state_data["weight"], 2)
        return state_sales

    def process_data(self, years: list[str]) -> RetailReport:
        """Process retail sales report."""
        try:
            sales_data = {}

            for year in years:
                for category in self.categories:
                    state_weights = self.fetch_cbp_data(category)
                    if not state_weights:
                        continue

                    marts_data = self.fetch_marts_data(year, category)
                    if marts_data.empty:
                        continue

                    for _, row in marts_data.iterrows():
                        month = str(row["time"])
                        # Fix: Remove year from month value if present
                        if len(month) == DATE_FORMAT_LENGTH:  # Format: YYYYMM
                            month = month[-2:]
                        month_key = f"{month.zfill(2)}"  # Will now correctly produce "2024-01"

                        national_sales = float(row["cell_value"])

                        # Initialize month data if it doesn't exist
                        if month_key not in sales_data:
                            sales_data[month_key] = MonthData(
                                states={},
                                national_total=CategoryTotal(category_445=0.0, category_448=0.0),
                            )

                        # Update national total
                        if category == "445":
                            sales_data[month_key].national_total.category_445 = national_sales
                        else:
                            sales_data[month_key].national_total.category_448 = national_sales

                        # Calculate and update state sales
                        state_sales = self.calculate_state_sales(national_sales, state_weights)
                        for state_code, sales_value in state_sales.items():
                            if state_code not in self.state_id_to_abbreviation:
                                continue

                            state_abbr = self.state_id_to_abbreviation[state_code]
                            if state_abbr not in sales_data[month_key].states:
                                sales_data[month_key].states[state_abbr] = StateData()

                            # Ensure values are Python floats
                            sales_obj = Sales(
                                sales_value=float(sales_value),
                                state_share=float(state_weights[state_code]["weight"]),
                            )

                            if category == "445":
                                sales_data[month_key].states[state_abbr].category_445 = sales_obj
                            else:
                                sales_data[month_key].states[state_abbr].category_448 = sales_obj

            metadata = Metadata(
                last_updated=datetime.now(tz=EASTERN).strftime("%Y-%m-%d"),
                categories=self.categories,
            )

            return RetailReport(metadata=metadata, sales_data=sales_data)

        except ValueError:
            self.logger.exception("Error in main processing")
            empty_month_data = MonthData(
                states={}, national_total=CategoryTotal(category_445=0.0, category_448=0.0)
            )
            return RetailReport(
                metadata=Metadata(
                    last_updated=datetime.now(tz=EASTERN).strftime("%Y-%m-%d"),
                    categories=self.categories,
                ),
                sales_data={"error": empty_month_data},
            )

    def validate_data(self, data: RetailReport) -> tuple[bool, dict]:
        """Validate the generated data for completeness and consistency."""
        try:
            validation_results = {}

            for month, month_data in data.sales_data.items():
                # Check completeness: Are all expected states present?
                expected_states = set(self.state_id_to_abbreviation.values())
                actual_states = set(month_data.states.keys())
                missing_states = list(expected_states - actual_states)

                # Check consistency: Are the share sums within the valid range?
                share_445_sum = sum(
                    state_data.category_445.state_share
                    for state_data in month_data.states.values()
                    if state_data.category_445
                )
                share_448_sum = sum(
                    state_data.category_448.state_share
                    for state_data in month_data.states.values()
                    if state_data.category_448
                )

                month_validation = {
                    "national_totals_present": bool(month_data.national_total),
                    "states_complete": len(missing_states) == 0,
                    "shares_valid": LOW_DEVIATION <= share_445_sum <= HIGH_DEVIATION
                    and LOW_DEVIATION <= share_448_sum <= HIGH_DEVIATION,
                    "missing_states": missing_states,
                }

                validation_results[month] = month_validation

            # Overall validation result: Are all months complete and consistent?
            is_valid = all(
                v["states_complete"] and v["shares_valid"] for v in validation_results.values()
            )

        except Exception as e:
            self.logger.exception("Error in validation")
            return False, {"error": str(e)}

        return is_valid, validation_results

    def save_data(self, data: RetailReport, validation_results: dict) -> None:
        """Save processed data and validation results as JSON."""
        try:
            output_dir = Path("data/retail_sales")
            Path.mkdir(output_dir, exist_ok=True)

            # Save main data
            data_file = Path(f"{output_dir}/retail_sales.json")
            data_dict = data.to_dict()
            with Path.open(data_file, "w") as f:
                json.dump(data_dict, f, indent=2)
            self.logger.info(f"Data saved to {data_file}")

            # Save validation results
            validation_file = Path(f"{output_dir}/retail_sales_validation.json")
            with Path.open(validation_file, "w") as f:
                json.dump(validation_results, f, indent=2)
            self.logger.info(f"Validation results saved to {validation_file}")

        except FileNotFoundError:
            self.logger.exception("Error saving data")


def main() -> None:
    """Execute the retail sales data processing."""
    try:
        api_key = os.getenv("CENSUS_API_KEY") or ""

        processor = RetailSalesProcessor(api_key)
        current_year = datetime.now(tz=EASTERN).year

        data = processor.process_data([str(current_year)])
        is_valid, validation_results = processor.validate_data(data)
        if is_valid:
            processor.save_data(data, validation_results)

    except Exception as e:
        raise ValueError("Error in main execution") from e


if __name__ == "__main__":
    main()
