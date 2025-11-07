"""
run_my_cleaning.py

Starter script to clean a single dataset using the DataScrubber class.
This version focuses on customers.csv and can be expanded once validated.
"""

import os
import pandas as pd
from utils.data_scrubber import DataScrubber

RAW_DIR = "data/raw"
CLEANED_DIR = "data/cleaned"


def clean_customers():
    """
    Load, clean, and save the customers dataset.
    """
    input_path = os.path.join(RAW_DIR, "customers.csv")
    output_path = os.path.join(CLEANED_DIR, "customers_cleaned.csv")

    print(f"\n📥 Loading: {input_path}")
    df = pd.read_csv(input_path)

    scrubber = DataScrubber(df)

    # Apply basic cleaning steps
    scrubber.standardize_column_names()
    scrubber.remove_duplicate_records()
    scrubber.handle_missing_data(fill_value="N/A")
    scrubber.rename_columns({"cust_id": "customer_id", "cust_name": "customer_name"})
    scrubber.format_column_strings("customer_name", case="lower")

    # Inspect results
    info, summary = scrubber.inspect()
    print("\n📊 Customers Dataset Info")
    print(info)
    print(summary)

    print(f"\n💾 Saving: {output_path}")
    scrubber.df.to_csv(output_path, index=False)


if __name__ == "__main__":
    clean_customers()
