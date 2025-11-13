"""Module 2: Initial Script to Verify Project Setup.

File: src/analytics_project/data_prep.py.
"""

"""Module 2: Initial Script to Verify Project Setup.

File: src/analytics_project/data_prep.py.
"""

# Standard library imports
import logging
import pathlib
from pathlib import Path

# Third-party imports
import numpy as np
import pandas as pd

# Local imports
from .utils_logger import logger, project_root

# Set up paths as constants
DATA_DIR: pathlib.Path = project_root.joinpath("data")
RAW_DATA_DIR: pathlib.Path = DATA_DIR.joinpath("raw")


# Define a reusable function that accepts a full path.
def read_and_log(path: pathlib.Path) -> pd.DataFrame:
    """Read a CSV at the given path into a DataFrame, with friendly logging.

    We know reading a csv file can fail
    (the file might not exist, it could be corrupted),
    so we put the statement in a try block.
    It could fail due to a FileNotFoundError or other exceptions.
    If it succeeds, we log the shape of the DataFrame.
    If it fails, we log an error and return an empty DataFrame.
    """
    try:
        # Typically, we log the start of a file read operation
        logger.info(f"Reading raw data from {path}.")
        df = pd.read_csv(path)
        # Typically, we log the successful completion of a file read operation
        logger.info(
            f"{path.name}: loaded DataFrame with shape {df.shape[0]} rows x {df.shape[1]} cols"
        )
        return df
    except FileNotFoundError:
        logger.error(f"File not found: {path}")
        return pd.DataFrame()
    except Exception as e:
        logger.error(f"Error reading {path}: {e}")
        return pd.DataFrame()


# Setup logger
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

RAW_DATA_DIR = Path("data/raw")


def drop_enrichment_columns(df):
    suffixes = ("_int", "_str", "_pct", "_units")
    cols_to_drop = [col for col in df.columns if col.endswith(suffixes)]
    return df.drop(columns=cols_to_drop, errors="ignore")


def enrich_customers(path):
    df = pd.read_csv(path)
    df = drop_enrichment_columns(df)

    # Add LoyaltyPoints (0–5000), inject NaN and negative
    df["LoyaltyPoints"] = np.random.randint(0, 5001, size=len(df))
    df.loc[df.sample(frac=0.03).index, "LoyaltyPoints"] = np.nan
    df.loc[0, "LoyaltyPoints"] = -150

    # Add EngagementStyle (Mobile/Desktop/InStore), inject typo and unknown
    df["EngagementStyle"] = np.random.choice(["Mobile", "Desktop", "InStore"], size=len(df))
    df.loc[df.sample(frac=0.02).index, "EngagementStyle"] = np.nan
    df.loc[1, "EngagementStyle"] = "Mobile "
    df.loc[2, "EngagementStyle"] = "Kiosk"

    df.to_csv(path, index=False)
    print(f" Enriched: {path.name}")


def enrich_products(path):
    df = pd.read_csv(path)
    df = drop_enrichment_columns(df)

    # Add StockLevel (100–1000), inject NaN and negative
    df["StockLevel"] = np.random.randint(100, 1001, size=len(df))
    df.loc[df.sample(frac=0.03).index, "StockLevel"] = np.nan
    df.loc[0, "StockLevel"] = -50

    # Add SupplierTier (Basic/Preferred/Premium), inject typo and unknown
    df["SupplierTier"] = np.random.choice(["Basic", "Preferred", "Premium"], size=len(df))
    df.loc[df.sample(frac=0.02).index, "SupplierTier"] = np.nan
    df.loc[1, "SupplierTier"] = "Preferred "
    df.loc[2, "SupplierTier"] = "UnknownTier"

    df.to_csv(path, index=False)
    print(f"✅ Enriched: {path.name}")


def enrich_sales(path):
    df = pd.read_csv(path)
    df = drop_enrichment_columns(df)

    # Add DiscountPercent (0–30), inject NaN and outlier
    df["DiscountPercent"] = np.round(np.random.uniform(0, 30, size=len(df)), 2)
    df.loc[df.sample(frac=0.03).index, "DiscountPercent"] = np.nan
    df.loc[0, "DiscountPercent"] = 150.0

    # Add PaymentMethod (CreditCard/PayPal/WireTransfer/GiftCard), inject typo and unknown
    df["PaymentMethod"] = np.random.choice(
        ["CreditCard", "PayPal", "WireTransfer", "GiftCard"], size=len(df)
    )
    df.loc[df.sample(frac=0.02).index, "PaymentMethod"] = np.nan
    df.loc[1, "PaymentMethod"] = "Credit Card"
    df.loc[2, "PaymentMethod"] = "Bitcoin"

    df.to_csv(path, index=False)
    print(f" Enriched: {path.name}")


def main():
    enrich_customers(RAW_DATA_DIR / "customers_data.csv")
    enrich_products(RAW_DATA_DIR / "products_data.csv")
    enrich_sales(RAW_DATA_DIR / "sales_data.csv")
    logger.info("🎉 All files enriched with realistic anomalies.")


if __name__ == "__main__":
    main()
