from pathlib import Path

import pandas as pd

# Define paths
RAW_DATA_DIR = Path("data/raw")
customer_path = RAW_DATA_DIR / "customers_data.csv"
product_path = RAW_DATA_DIR / "products_data.csv"
sales_path = RAW_DATA_DIR / "sales_data.csv"

# Define original columns for each file
customer_columns = ["CustomerID", "Name", "Region", "JoinDate"]
product_columns = ["ProductID", "ProductName", "Category", "UnitPrice"]
sales_columns = [
    "TransactionID",
    "SaleDate",
    "CustomerID",
    "ProductID",
    "StoreID",
    "CampaignID",
    "SaleAmount",
]


def restore_original_columns(path, original_columns):
    df = pd.read_csv(path)
    df_cleaned = df[original_columns]
    df_cleaned.to_csv(path, index=False)
    print(f"✅ Cleaned file saved: {path.name} with columns: {original_columns}")


def main():
    """Restore original column names for customer, product, and sales datasets."""
    restore_original_columns(customer_path, customer_columns)
    restore_original_columns(product_path, product_columns)
    restore_original_columns(sales_path, sales_columns)


if __name__ == "__main__":
    main()
