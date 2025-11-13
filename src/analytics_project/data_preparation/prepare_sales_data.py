"""prepare_sales_data.py.

This script reads sales data from the data/raw folder, cleans the data using
the DataScrubber utility, and writes the cleaned version to the data/prepared folder.

Operations:
- Standardize column names and data types
- Remove duplicates and outliers
- Handle missing values
- Standardize payment methods
- Validate sale amounts
- Save cleaned data
"""

from analytics_project.utils.logger import init_logger, logger, project_root

init_logger(level="INFO")

import sys
import pandas as pd
from analytics_project.data_preparation.data_scrubber import DataScrubber

# Add project root to sys.path for local imports
sys.path.append(str(project_root))

# Configure paths
DATA_DIR = project_root / "data"
RAW_DATA_DIR = DATA_DIR / "raw"
PREPARED_DATA_DIR = DATA_DIR / "prepared"

# Ensure directories exist
for directory in [DATA_DIR, RAW_DATA_DIR, PREPARED_DATA_DIR]:
    directory.mkdir(exist_ok=True)


#####################################
# Define Main Function - The main entry point of the script
#####################################


def clean_sales_data(input_file: str = "sales_data.csv") -> pd.DataFrame:
    """Clean sales data using DataScrubber.

    Parameters
    ----------
    input_file : str, optional
        Name of input file in raw data directory, by default "sales_data.csv"

    Returns
    -------
    pd.DataFrame
        Cleaned DataFrame with standardized formats and removed invalid entries.
        The following operations are performed:
        - Column names standardized
        - Duplicate records removed
        - Missing values and invalid entries handled
        - Payment methods standardized
        - Outlier sale amounts filtered
        - Rows with missing critical fields removed
        - Campaign IDs converted to integers where present
    """
    logger.info("=== Starting sales data cleaning ===")

    # Read raw data
    file_path = RAW_DATA_DIR / input_file
    logger.info("Reading data from: %s", file_path)
    df = pd.read_csv(file_path)
    logger.info("Initial data shape: %s", df.shape)

    # Initialize DataScrubber with raw data
    scrubber = DataScrubber(df)
    scrubber.standardize_column_names()
    logger.info("After standardization: %s", scrubber.df.columns.tolist())

    scrubber.remove_duplicate_records()
    scrubber.handle_missing_data(drop=True)
    # Parse sale date
    scrubber.parse_date_column("sale_date", new_column="sale_date")
    scrubber.filter_outliers("discount_percent", 0, 100)
    scrubber.override_invalid_dates("sale_date", fixed_date="2025-05-04")

    # Convert sale_amount to float before filtering
    if "sale_amount" in scrubber.df.columns:
        scrubber.df["sale_amount"] = (
            scrubber.df["sale_amount"]
            .astype(str)
            .str.replace(r"[^\d\.]", "", regex=True)
            .replace("", pd.NA)
        )
        scrubber.df["sale_amount"] = pd.to_numeric(scrubber.df["sale_amount"], errors="coerce")

        max_val = scrubber.df["sale_amount"].quantile(0.99)
        scrubber.filter_outliers("sale_amount", min_val=0, max_val=max_val)
    else:
        logger.warning("sale_amount column not found — skipping outlier filtering.")

    # Convert campaign_id if present
    if "campaign_id" in scrubber.df.columns:
        scrubber.df["campaign_id"] = pd.to_numeric(
            scrubber.df["campaign_id"], errors="coerce"
        ).astype("Int64")  # Nullable integer type
    logger.info("Safely converted 'campaign_id' to integer format, preserving 0 as 'no campaign'")

    # Get cleaned data
    df_cleaned = scrubber.df

    # Log data quality metrics
    info, summary = scrubber.inspect()
    logger.info("Data info:\n%s", info)
    logger.info("Data summary:\n%s", summary)
    logger.info("=== Finished sales data cleaning ===")

    return df_cleaned


def main() -> None:
    """Execute the sales data preparation process.

    This function orchestrates the data preparation process:
    1. Cleans the sales data using DataScrubber
    2. Saves the cleaned data to a CSV file
    3. Logs data quality metrics and summary statistics

    If an error occurs during any step, it logs the error details and
    raises the exception for proper handling by the caller.
    """
    try:
        logger.info("Starting sales data preparation process")

        # Clean the data
        df = clean_sales_data()
        logger.info("Final column names in main(): %s", df.columns.tolist())
        logger.info("Standardized columns: %s", df.columns.tolist())

        if "payment_method" in df.columns:
            payment_map = {
                "creditcard": "Credit Card",
                "paypal": "PayPal",
                "giftcard": "GiftCard",
                "bitcoin": pd.NA,  # Invalid payment method
            }

            df["payment_method"] = (
                df["payment_method"].astype(str).str.replace(" ", "").str.lower().map(payment_map)
            )

            df = df[df["payment_method"].notna()]
            logger.info("Standardized payment methods: %s", df["payment_method"].unique())
        else:
            logger.warning("payment_method column not found — skipping standardization.")

        if "sale_amount" in df.columns:
            df["sale_amount"] = (
                df["sale_amount"]
                .astype(str)
                .str.replace(r"[^\d\.]", "", regex=True)  # Remove symbols like ?, $, #
                .replace("", pd.NA)
                .astype(float)
            )

            df = df[df["sale_amount"].notna() & (df["sale_amount"] >= 0)]
            logger.info(
                "Cleaned sale_amount column — min: %.2f, max: %.2f",
                df["sale_amount"].min(),
                df["sale_amount"].max(),
            )
        else:
            logger.warning("sale_amount column not found — skipping symbol cleanup.")

        empty_count = (df == "").sum().sum()
        logger.info("Converted %d empty strings to missing values", empty_count)

        # Verify data quality
        if df.empty:
            raise ValueError("Data cleaning resulted in empty DataFrame")

        # Create output directory if it doesn't exist
        PREPARED_DATA_DIR.mkdir(parents=True, exist_ok=True)

        # Save results
        output_file = PREPARED_DATA_DIR / "sales_data_prepared.csv"
        df.to_csv(output_file, index=False)
        logger.info("Saved cleaned data to: %s", output_file)

        # Log summary statistics
        logger.info("Final data shape: %s", df.shape)
        if "payment_method" in df.columns:
            logger.info("Unique payment methods: %s", df["payment_method"].unique())
        if "sale_amount" in df.columns:
            logger.info(
                "Sale amount range: %.2f to %.2f",
                df["sale_amount"].min(),
                df["sale_amount"].max(),
            )

        logger.info("Sales data preparation completed successfully")

    except FileNotFoundError as e:
        logger.error("Could not find input data file: %s", e)
        raise

    except ValueError as e:
        logger.error("Data validation error: %s", e)
        raise

    except Exception as e:
        logger.error("Unexpected error during sales data preparation: %s", e)
        raise


if __name__ == "__main__":
    main()
