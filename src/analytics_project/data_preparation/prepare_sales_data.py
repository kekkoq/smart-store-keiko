"""
scripts/data_preparation/prepare_sales.py

This script reads data from the data/raw folder, cleans the data,
and writes the cleaned version to the data/prepared folder.

Tasks:
- Remove duplicates
- Handle missing values
- Remove outliers
- Ensure consistent formatting

"""

#####################################
# Import Modules at the Top
#####################################

# Import from Python Standard Library
import pathlib
import sys

# Import from external packages (requires a virtual environment)
import pandas as pd

# Ensure project root is in sys.path for local imports (now 3 parents are needed)
sys.path.append(str(pathlib.Path(__file__).resolve().parent.parent.parent))

# Import local modules (e.g. utils/logger.py)
from analytics_project.utils.logger import logger

# Optional: Use a data_scrubber module for common data cleaning tasks
# from analytics_project.utils.data_scrubber import DataScrubber

# Constants
SCRIPTS_DIR = pathlib.Path(__file__).resolve().parent
PROJECT_ROOT = SCRIPTS_DIR.parent.parent.parent
DATA_DIR = PROJECT_ROOT / "data"
RAW_DATA_DIR = DATA_DIR / "raw"
PREPARED_DATA_DIR = DATA_DIR / "prepared"

# Ensure the directories exist or create them
DATA_DIR.mkdir(exist_ok=True)
RAW_DATA_DIR.mkdir(exist_ok=True)
PREPARED_DATA_DIR.mkdir(exist_ok=True)

#####################################
# Define Functions - Reusable blocks of code / instructions
#####################################

# TODO: Complete this by implementing functions based on the logic in the other scripts


def read_raw_data(file_name: str) -> pd.DataFrame:
    """
    Read raw data from CSV.

    Args:
        file_name (str): Name of the CSV file to read.

    Returns:
        pd.DataFrame: Loaded DataFrame.
    """
    logger.info("FUNCTION START: read_data with file_name=%s", file_name)

    file_path = RAW_DATA_DIR / file_name
    logger.info("Reading data from %s", file_path)

    df = pd.read_csv(file_path)
    logger.info("Loaded dataframe with %d rows and %d columns", len(df), len(df.columns))
    logger.info("Sample rows:\n%s", df.head(3).to_string(index=False))

    return df


def clean_sales_data(df: pd.DataFrame) -> pd.DataFrame:
    logger.info(f"FUNCTION START: clean_sales_data with shape: {df.shape}")

    # Strip whitespace from all string columns
    df = df.apply(lambda col: col.str.strip() if col.dtype == "object" else col)

    # Replace invalid symbols and blanks
    df.replace({"?": pd.NA, "": pd.NA}, inplace=True)

    # Standardize PaymentMethod
    if "PaymentMethod" in df.columns:
        original_methods = df["PaymentMethod"].dropna().unique().tolist()

    # Normalize formatting
    df["PaymentMethod"] = df["PaymentMethod"].str.replace(" ", "").str.title()

    # Replace known variants and mark invalids
    df["PaymentMethod"] = df["PaymentMethod"].replace(
        {
            "Creditcard": "Credit Card",
            "Paypal": "PayPal",
            "Giftcard": "GiftCard",
            "Bitcoin": pd.NA,  # Not a valid method
        }
    )
    # Drop rows with invalid payment methods
    df = df[df["PaymentMethod"].notna()]

    # Convert CampaignID to whole numbers
    if "CampaignID" in df.columns:
        df["CampaignID"] = pd.to_numeric(df["CampaignID"], errors="coerce").fillna(0).astype(int)
        logger.info("Converted CampaignID to whole numbers")

    # Drop rows with critic al missing values
    df.dropna(
        subset=["TransactionID", "SaleDate", "CustomerID", "ProductID", "SaleAmount"], inplace=True
    )

    return df

    # TODO: OPTIONAL Add data profiling here to understand the dataset
    # Suggestion: Log the datatypes of each column and the number of unique values
    # Example:
    # logger.info(f"Column datatypes: \n{df.dtypes}")
    # logger.info(f"Number of unique values: \n{df.nunique()}")

    return df


#####################################
# Define Main Function - The main entry point of the script
#####################################


def main() -> pd.DataFrame:
    input_file = "sales_data.csv"
    df = read_raw_data(input_file)
    df = clean_sales_data(df)
    return df

    """
    Main function for processing data.
    """
    logger.info("==================================")
    logger.info("STARTING prepare_sales_data.py")
    logger.info("==================================")

    logger.info(f"Root         : {PROJECT_ROOT}")
    logger.info(f"data/raw     : {RAW_DATA_DIR}")
    logger.info(f"data/prepared: {PREPARED_DATA_DIR}")
    logger.info(f"scripts      : {SCRIPTS_DIR}")

    input_file = "sales_data.csv"
    output_file = "sales_prepared.csv"

    # Read raw data
    df = read_raw_data(input_file)

    # Record original shape
    original_shape = df.shape

    # Log initial dataframe information
    logger.info(f"Initial dataframe columns: {', '.join(df.columns.tolist())}")
    logger.info(f"Initial dataframe shape: {df.shape}")

    # Clean column names
    original_columns = df.columns.tolist()
    df.columns = df.columns.str.strip()

    # Log if any column names changed
    changed_columns = [
        f"{old} -> {new}" for old, new in zip(original_columns, df.columns) if old != new
    ]
    if changed_columns:
        logger.info(f"Cleaned column names: {', '.join(changed_columns)}")

    # TODO: Remove duplicates

    # TODO:Handle missing values

    # TODO:Remove outliers

    logger.info("==================================")
    logger.info(f"Original shape: {df.shape}")
    logger.info(f"Cleaned shape:  {original_shape}")
    logger.info("==================================")
    logger.info("FINISHED prepare_sales_data.py")
    logger.info("==================================")


#####################################
# Conditional Execution Block
# Ensures the script runs only when executed directly
# This is a common Python convention.
#####################################

if __name__ == "__main__":
    df = main()

    # Save prepared data to data/prepared folder

    output_file = PREPARED_DATA_DIR / "sales_data_prepared.csv"
    df.to_csv(output_file, index=False)
    logger.info("Saved cleaned data to %s", output_file)
