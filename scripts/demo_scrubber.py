import sys
import os
import pandas as pd

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "src")))

from analytics_project.data_preparation.data_scrubber import DataScrubber

# Ensure src/ is on the Python path
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..")))

# Create output directory if it doesn't exist
os.makedirs("data/cleaned", exist_ok=True)

# Define input/output paths
datasets = {
    "customers": "data/raw/customers_data.csv",
    "products": "data/raw/products_data.csv",
    "sales": "data/raw/sales_data.csv",
}

for name, path in datasets.items():
    print(f"\n Loading: {path}")
    df = pd.read_csv(path)
    scrubber = DataScrubber(df)

    # Apply cleaning steps
    scrubber.standardize_column_names()
    scrubber.handle_missing_data(fill_value="Unknown")
    scrubber.remove_duplicate_records()

    # Inspect and save
    print(f" {name.capitalize()} Dataset Info")
    scrubber.inspect()
    output_path = f"data/cleaned/{name}_cleaned.csv"
    scrubber.save(output_path)
    print(f" Saved: {output_path}")
