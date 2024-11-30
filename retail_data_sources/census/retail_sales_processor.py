"""Retail sales data processor using Census API."""

import json
import os
from datetime import datetime

import pandas as pd
import requests

from retail_data_sources.census.models.retail_sales import ValidationMetrics
from retail_data_sources.census.utils import setup_logging


class RetailSalesProcessor:
    """Retail sales data processor using Census API."""

    state_id_to_abbreviation = {
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

            return sales_df

        except Exception as e:
            self.logger.error(f"Error fetching MARTS data for {year}, category {category}: {e}")
            return pd.DataFrame()

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

            return state_weights

        except Exception as e:
            self.logger.error(f"Error fetching CBP data for category {category}: {e}")
            return {}

    def calculate_state_sales(
        self, national_sales: float, state_weights: dict[str, dict[str, float]]
    ) -> dict[str, float]:
        """Calculate state-level sales using weights."""
        state_sales = {}
        for state_code, state_data in state_weights.items():
            state_sales[state_code] = round(national_sales * state_data["weight"], 2)
        return state_sales

    def process_retail_data(self, years: list[str]) -> dict:
        """Main processing function to generate retail sales data."""
        try:
            result = {
                "metadata": {
                    "last_updated": datetime.now().strftime("%Y-%m-%d"),
                    "categories": self.categories,
                },
                "sales_data": {},
            }

            for category in self.categories:
                state_weights = self.fetch_cbp_data(category)
                if not state_weights:
                    continue

                for year in years:
                    marts_data = self.fetch_marts_data(year, category)
                    if marts_data.empty:
                        continue

                    for _, row in marts_data.iterrows():
                        month = row["time"]
                        national_sales = float(row["cell_value"])

                        if month not in result["sales_data"]:
                            result["sales_data"][month] = {"states": {}, "national_total": {}}

                        result["sales_data"][month]["national_total"][category] = national_sales
                        state_sales = self.calculate_state_sales(national_sales, state_weights)

                        for state_code, sales_value in state_sales.items():
                            state_abbr = self.state_id_to_abbreviation.get(state_code, state_code)
                            if state_abbr not in result["sales_data"][month]["states"]:
                                result["sales_data"][month]["states"][state_abbr] = {}

                            result["sales_data"][month]["states"][state_abbr][category] = {
                                "sales_value": sales_value,
                                "state_share": state_weights[state_code]["weight"],
                            }

            return result

        except Exception as e:
            self.logger.error(f"Error in main processing: {e}")
            return {}

    def validate_data(self, data: dict) -> ValidationMetrics:
        """Validate the generated data."""
        try:
            expected_states = 50
            expected_categories = len(self.categories)

            total_records = 0
            expected_records = 0

            for month_data in data["sales_data"].values():
                total_records += sum(
                    len(state_data) for state_data in month_data["states"].values()
                )
                expected_records += expected_states * expected_categories

            completeness = total_records / expected_records if expected_records > 0 else 0
            consistency = 1.0

            for month_data in data["sales_data"].values():
                for category in self.categories:
                    national_total = month_data["national_total"].get(category, 0)
                    state_sum = sum(
                        state_data.get(category, {}).get("sales_value", 0)
                        for state_data in month_data["states"].values()
                    )

                    if national_total > 0:
                        diff_percentage = abs(state_sum - national_total) / national_total
                        if diff_percentage > 0.02:
                            consistency -= 0.1

            return ValidationMetrics(
                completeness=round(completeness, 3),
                consistency=round(max(0, consistency), 3),
                timestamp=datetime.now().isoformat(),
            )

        except Exception as e:
            self.logger.error(f"Error in validation: {e}")
            return ValidationMetrics(
                completeness=0.0, consistency=0.0, timestamp=datetime.now().isoformat()
            )

    def save_data(self, data: dict, validation: ValidationMetrics) -> None:
        """Save processed data and validation results."""
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        output_dir = "../output"
        os.makedirs(output_dir, exist_ok=True)

        data_file = f"{output_dir}/retail_sales_{timestamp}.json"
        with open(data_file, "w") as f:
            json.dump(data, f, indent=2)

        validation_file = f"{output_dir}/validation_{timestamp}.json"
        with open(validation_file, "w") as f:
            json.dump(validation.__dict__, f, indent=2)

        self.logger.info(f"Data saved to {data_file}")
        self.logger.info(f"Validation results saved to {validation_file}")


def main() -> None:
    """Main function to execute the retail sales data processing."""
    # Get Census API key
    api_key = os.getenv("CENSUS_API_KEY")
    if not api_key:
        raise ValueError("Census API key not found in environment variables")

    # Initialize processor
    processor = RetailSalesProcessor(api_key)

    # Process last 5 years
    current_year = datetime.now(tz=datetime.timezone.utc).year
    years = [str(year) for year in range(current_year - 5, current_year)]
    try:
        # Process data
        data = processor.process_retail_data(years)

        if data:
            # Validate
            validation = processor.validate_data(data)

            # Save results
            processor.save_data(data, validation)

    except Exception as e:
        raise ValueError("Error in main execution") from e


if __name__ == "__main__":
    # Run the main function
    main()
