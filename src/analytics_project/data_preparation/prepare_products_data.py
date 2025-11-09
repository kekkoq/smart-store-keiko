"""prepare_products_data.py.

This script reads products data from the data/raw folder, cleans the data using
the DataScrubber utility, and writes the cleaned version to the data/prepared folder.

Operations:
- Standardize column names and data types
- Remove duplicates and outliers
- Handle missing values
- Standardize supplier tiers
- Validate stock levels
- Save cleaned data
"""

import pathlib
import sys

import pandas as pd

# Add project root to sys.path for local imports
sys.path.append(str(pathlib.Path(__file__).resolve().parent.parent.parent))

from analytics_project.utils.logger import logger
from analytics_project.data_preparation.data_scrubber import DataScrubber

# Configure paths
SCRIPTS_DIR = pathlib.Path(__file__).resolve().parent
PROJECT_ROOT = SCRIPTS_DIR.parent.parent.parent
DATA_DIR = PROJECT_ROOT / "data"
RAW_DATA_DIR = DATA_DIR / "raw"
PREPARED_DATA_DIR = DATA_DIR / "prepared"

# Ensure directories exist
for directory in [DATA_DIR, RAW_DATA_DIR, PREPARED_DATA_DIR]:
    directory.mkdir(exist_ok=True)


def read_raw_data(file_name: str) -> pd.DataFrame:
    file_path = RAW_DATA_DIR / file_name
    try:
        logger.info(f"READING: {file_path}")
        return pd.read_csv(file_path)
    except FileNotFoundError:
        logger.error(f"File not found: {file_path}")
        return pd.DataFrame()
    except Exception as e:
        logger.error(f"Error reading {file_path}: {e}")
        return pd.DataFrame()


def save_prepared_data(df: pd.DataFrame, file_name: str) -> None:
    file_path = PREPARED_DATA_DIR / file_name
    logger.info(f"Saving cleaned data to: {file_path}")
    df.to_csv(file_path, index=False)


def remove_duplicates(df: pd.DataFrame) -> pd.DataFrame:
    logger.info(f"Removing duplicates from shape: {df.shape}")
    df_deduped = df.drop_duplicates()
    logger.info(f"After deduplication: {df_deduped.shape}")
    return df_deduped


def handle_missing_values(df: pd.DataFrame) -> pd.DataFrame:
    logger.info(f"FUNCTION START: handle_missing_values with dataframe shape={df.shape}")

    missing_before = df.isna().sum().sum()
    logger.info(f"Total missing values before handling: {missing_before}")

    if "stock_level" in df.columns:
        median_points = df["stock_level"].median()
        df.loc[:, "stock_level"] = df["stock_level"].fillna(median_points)

    if "supplier_tier" in df.columns and not df["supplier_tier"].mode().empty:
        mode_style = df["supplier_tier"].mode()[0]
        df.loc[:, "supplier_tier"] = df["supplier_tier"].fillna(mode_style)

    missing_after = df.isna().sum().sum()
    logger.info(f"Total missing values after handling: {missing_after}")
    return df


def clean_values(df: pd.DataFrame) -> pd.DataFrame:
    logger.info(f"FUNCTION START: clean values with shape: {df.shape}")

    # Remove negative stock_level
    if "stock_level" in df.columns:
        negative_count = (df["stock_level"] < 0).sum()
        df = df[df["stock_level"] >= 0].copy()
        logger.info(f"Removed {negative_count} rows with negative stock_level")

        df["stock_level"] = df["stock_level"].astype(int)
        logger.info("Converted stock_level to integer type")

    # Standardize supplier_tier values
    if "supplier_tier" in df.columns:
        original_tiers = df["supplier_tier"].dropna().unique().tolist()

        # Normalize whitespace and casing
        df["supplier_tier"] = df["supplier_tier"].astype(str).str.strip().str.title()

        # Replace 'Unknown' with 'Standard'
        df["supplier_tier"] = df["supplier_tier"].replace({"Unknown": "Standard"})

        # Validate against known tiers
        tier_map = {
            "Basic": "Basic",
            "Preferred": "Preferred",
            "Premium": "Premium",
            "Standard": "Standard",  # Include Standard as valid tier
        }
        df["supplier_tier"] = df["supplier_tier"].map(tier_map).fillna("Standard")

        updated_tiers = df["supplier_tier"].unique().tolist()
        logger.info(f"Standardized supplier_tier from {original_tiers} to {updated_tiers}")

    return df


def remove_outliers(df: pd.DataFrame) -> pd.DataFrame:
    logger.info(f"Removing outliers from shape: {df.shape}")
    if "stock_level" in df.columns:
        mean = df["stock_level"].mean()
        std = df["stock_level"].std()
        lower = mean - 3 * std
        upper = mean + 3 * std
        original_count = len(df)
        df = df[df["stock_level"].between(lower, upper)]
        removed = original_count - len(df)
        logger.info(f"Outliers removed: {removed}")
    return df


# ================================
# Main
# ================================


def clean_products_data(input_file: str = "products_data.csv") -> pd.DataFrame:
    """Clean product data using DataScrubber.

    Parameters
    ----------
    input_file : str, optional
        Name of input file in raw data directory, by default "products_data.csv"

    Returns
    -------
    pd.DataFrame
        Cleaned product data with standardized formats and validated stock levels
    """
    logger.info("=== Starting product data cleaning ===")

    # Read raw data
    file_path = RAW_DATA_DIR / input_file
    logger.info("Reading data from: %s", file_path)
    df = pd.read_csv(file_path)
    logger.info("Initial data shape: %s", df.shape)

    # Initialize DataScrubber with raw data
    scrubber = DataScrubber(df)

    # Apply cleaning operations in sequence
    scrubber.standardize_column_names()
    scrubber.remove_duplicate_records()
    scrubber.handle_missing_data(fill_value=0)  # Fill missing stock with 0
    scrubber.filter_outliers(
        "stock_level",
        min_val=0,
        max_val=scrubber.df["stock_level"].quantile(0.99),  # ✅ Use scrubber.df here
    )

    logger.info("=== Finished product data cleaning ===")
    return scrubber.df


def main() -> None:
    """Execute the product data preparation process."""
    try:
        # Clean the data
        df = clean_products_data()

        if "supplier_tier" in df.columns:
            tier_map = {
                "basic": "Basic",
                "preferred": "Preferred",
                "premium": "Premium",
                "standard": "Standard",
                "unknown": "Standard",
            }

            df["supplier_tier"] = (
                df["supplier_tier"]
                .astype(str)
                .str.strip()
                .str.lower()
                .map(tier_map)
                .fillna("Standard")
            )
            logger.info("Standardized supplier tiers: %s", df["supplier_tier"].unique())
        else:
            logger.warning("supplier_tier column not found — skipping standardization.")

        # Save results
        output_file = PREPARED_DATA_DIR / "products_prepared.csv"
        df.to_csv(output_file, index=False)
        logger.info("Saved cleaned data to: %s", output_file)

        logger.info("Final data shape: %s", df.shape)
        logger.info("Unique supplier tiers: %s", df["supplier_tier"].unique())
        logger.info("Stock level range: %d to %d", df["stock_level"].min(), df["stock_level"].max())

    except Exception as e:
        logger.error("Failed to clean product data: %s", e)
        raise


if __name__ == "__main__":
    main()
