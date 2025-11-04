"""Module 2: Initial Script to Verify Project Setup.

File: src/analytics_project/data_prep.py.
"""

# Imports after the opening docstring

import pathlib

import pandas as pd

from .utils_logger import init_logger, logger, project_root

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


import pandas as pd
import numpy as np


# add two new columns to each dataset
def enrich_customers(path):
    df = pd.read_csv(path)

    df['LoyaltyPoints_int'] = np.random.randint(0, 5001, size=len(df))
    df['EngagementStyle_str'] = np.random.choice(['Mobile', 'Desktop', 'InStore'], size=len(df))

    # Inject ~5% missing values
    df.loc[df.sample(frac=0.05).index, 'LoyaltyPoints_int'] = np.nan

    # Inject false/inconsistent values
    df.loc[0, 'EngagementStyle_str'] = 'Mobile '  # trailing space
    df.loc[1, 'EngagementStyle_str'] = 'Kiosk'  # unexpected category
    df.loc[2, 'LoyaltyPoints_int'] = -100  # invalid negative value

    df.to_csv(path, index=False)
    print(f"Updated {path}")


def enrich_products(path):
    df = pd.read_csv(path)

    df['StockQuantity_units'] = np.random.randint(0, 1001, size=len(df))
    df['SupplierName_str'] = np.random.choice(
        ['Acme Corp', 'Global Supplies', 'Tech Distributors', 'HomeGoods Inc.'], size=len(df)
    )

    # Inject ~5% missing values
    df.loc[df.sample(frac=0.05).index, 'StockQuantity_units'] = np.nan

    # Inject false/inconsistent values
    df.loc[0, 'StockQuantity_units'] = -50  # invalid negative value
    df.loc[1, 'SupplierName_str'] = 'Acme Corp'  # unexpected space
    df.loc[2, 'SupplierName_str'] = ''  # empty string

    df.to_csv(path, index=False)
    print(f"Updated {path}")


def enrich_sales(path):
    df = pd.read_csv(path)

    df['DiscountPercent_pct'] = np.round(np.random.uniform(0, 30, size=len(df)), 2)
    df['PaymentType_str'] = np.random.choice(
        ['CreditCard', 'PayPal', 'WireTransfer', 'GiftCard'], size=len(df)
    )

    # Inject ~5% missing values
    df.loc[df.sample(frac=0.05).index, 'DiscountPercent_pct'] = np.nan

    # Inject false/inconsistent values
    df.loc[0, 'DiscountPercent_pct'] = 150.0  # invalid over 100%
    df.loc[1, 'PaymentType_str'] = 'Credit Card'  # unexpected space
    df.loc[2, 'PaymentType_str'] = 'Bitcoin'  # unexpected category

    df.to_csv(path, index=False)
    print(f"Updated {path}")

    # Define a main function to start our data processing pipeline.


def main() -> None:
    """Process raw data."""
    logger.info("Starting data preparation...")

    # Build explicit paths for each file under data/raw
    customer_path = RAW_DATA_DIR.joinpath("customers_data.csv")
    product_path = RAW_DATA_DIR.joinpath("products_data.csv")
    sales_path = RAW_DATA_DIR.joinpath("sales_data.csv")

    # Call the function once per file
    read_and_log(customer_path)
    read_and_log(product_path)
    read_and_log(sales_path)

    # Enrich datasets with new columns and inject inconsistencies
    enrich_customers(customer_path)
    enrich_products(product_path)
    enrich_sales(sales_path)

    logger.info("Data preparation complete.")
    # Standard Python idiom to run this module as a script when executed directly.


if __name__ == "__main__":
    # Initialize logger
    init_logger()

    # Call the main function by adding () after the function name
    main()
