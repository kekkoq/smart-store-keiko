"""prepare_customers_data.py.

This script reads customer data from the data/raw folder, cleans the data using
the DataScrubber utility, and writes the cleaned version to the data/prepared folder.

Operations:
- Standardize column names and data types
- Remove duplicates and outliers
- Handle missing values
- Standardize engagement styles
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


def clean_customers_data(input_file: str = "customers_data.csv") -> pd.DataFrame:
    """Clean customer data using DataScrubber.

    Parameters
    ----------
    input_file : str, optional
        Name of input file in raw data directory, by default "customers_data.csv"

    Returns
    -------
    pd.DataFrame
        Cleaned customer data with standardized formats and removed outliers
    """
    logger.info("=== Starting customer data cleaning ===")

    # Read raw data
    file_path = RAW_DATA_DIR / input_file
    logger.info("Reading data from: %s", file_path)
    df = pd.read_csv(file_path)
    logger.info("Initial data shape: %s", df.shape)
    logger.info("Loaded columns: %s", df.columns.tolist())

    # Log and convert empty strings before cleaning
    empty_count = (df == "").sum().sum()
    logger.info("Converted %d empty strings to missing values", empty_count)

    scrubber = DataScrubber(df)
    scrubber.convert_empty_strings_to_na()

    # Apply cleaning operations in sequence
    scrubber.standardize_column_names()
    scrubber.remove_duplicate_records()
    scrubber.handle_missing_data(fill_value=0)  # Fill missing loyalty points with 0
    scrubber.filter_outliers(
        "loyalty_points", min_val=0, max_val=scrubber.df["loyalty_points"].quantile(0.99)
    )

    logger.info("=== Finished customer data cleaning ===")
    return scrubber.df


def main() -> None:
    """Execute the customer data preparation process."""
    try:
        # Clean the data
        df = clean_customers_data()

        if "engagement_style" in df.columns:
            style_map = {
                "mobile": "Mobile",
                "tablet": "Mobile",
                "desktop": "Desktop",
                "instore": "InStore",
                "kiosk": "InStore",
            }

            df["engagement_style"] = (
                df["engagement_style"]
                .astype(str)
                .str.strip()
                .str.lower()
                .map(style_map)
                .fillna("Unknown")
            )
            logger.info("Standardized engagement styles: %s", df["engagement_style"].unique())
        else:
            logger.warning("engagement_style column not found — skipping standardization.")

        # Remove negative loyalty points
        scrubber = DataScrubber(df)
        scrubber.remove_negative_values("loyalty_points")

        # Save results
        output_file = PREPARED_DATA_DIR / "customers_prepared.csv"
        df.to_csv(output_file, index=False)
        logger.info("Saved cleaned data to: %s", output_file)

        # Log summary statistics
        logger.info("Final data shape: %s", df.shape)
        logger.info("Unique engagement styles: %s", df["engagement_style"].unique())
        logger.info(
            "Loyalty points range: %d to %d", df["loyalty_points"].min(), df["loyalty_points"].max()
        )
    except Exception as e:
        logger.error("Failed to clean customer data: %s", e)
        raise


if __name__ == "__main__":
    main()
