"""scripts/data_preparation/prepare_sales.py.

This script reads data from the data/raw folder, cleans the data,
and writes the cleaned version to the data/prepared folder.

Tasks:
- Remove duplicates
- Handle missing values
- Remove outliers
- Ensure consistent formatting

"""

#####################################
# Imports
#####################################

# Standard library imports
from pathlib import Path
import sys

# Third-party imports
import pandas as pd

# Adjust Python path for local imports
sys.path.append(str(Path(__file__).resolve().parent.parent.parent))

# Local imports
from analytics_project.utils.logger import logger

# Optional local imports
# from analytics_project.utils.data_scrubber import DataScrubber

# Constants
SCRIPTS_DIR = Path(__file__).resolve().parent
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


def read_raw_data(file_name: str) -> pd.DataFrame:
    """Read raw data from CSV file in the raw data directory.

    This function reads a CSV file from the raw data directory and returns it as
    a pandas DataFrame. It logs the loading process, including the file path,
    number of rows and columns loaded, and a sample of the data.

    Parameters
    ----------
    file_name : str
        Name of the CSV file to read from the raw data directory.
        The file should exist in RAW_DATA_DIR.

    Returns
    -------
    pd.DataFrame
        Loaded DataFrame containing the raw data from the CSV file.
        The DataFrame structure depends on the input file's content.

    Example
    -------
    >>> df = read_raw_data("sales_data.csv")
    >>> print(f"Loaded {len(df)} rows of data")
    """
    logger.info("FUNCTION START: read_data with file_name=%s", file_name)

    file_path = RAW_DATA_DIR / file_name
    logger.info("Reading data from %s", file_path)

    df = pd.read_csv(file_path)
    logger.info("Loaded dataframe with %d rows and %d columns", len(df), len(df.columns))
    logger.info("Sample rows:\n%s", df.head(3).to_string(index=False))

    return df


def clean_sales_data(df: pd.DataFrame) -> pd.DataFrame:
    """Clean and standardize sales data in the DataFrame.

    This function performs several cleaning operations on the sales data:
    - Strips whitespace from string columns
    - Handles missing values and invalid entries
    - Standardizes payment method formatting
    - Converts data types (e.g., CampaignID to integers)
    - Removes rows with zero sale amounts
    - Ensures critical fields are present

    Parameters
    ----------
    df : pd.DataFrame
        Raw sales data DataFrame with expected columns:
        - TransactionID
        - SaleDate
        - CustomerID
        - ProductID
        - SaleAmount
        Optional columns:
        - PaymentMethod
        - CampaignID

    Returns
    -------
    pd.DataFrame
        Cleaned DataFrame with standardized formats and removed invalid entries.
        Rows may be fewer than input due to filtering of invalid data.

    Notes
    -----
    - Rows with missing values in critical fields will be dropped
    - Zero sale amounts are considered invalid and removed
    - Payment methods are standardized (e.g., "Creditcard" -> "Credit Card")
    - Bitcoin is not accepted as a valid payment method
    """
    logger.info("START: clean_sales_data with df.shape = %s", df.shape)

    # Strip whitespace from all string columns
    df = df.apply(lambda col: col.str.strip() if col.dtype == "object" else col)

    # Replace blank strings and "?" with NaN
    df.replace(r"^\s*$", pd.NA, regex=True, inplace=True)
    df.replace("?", pd.NA, inplace=True)

    # Drop rows with any missing values
    before_dropna = df.shape[0]
    df.dropna(how="any", inplace=True)
    logger.info(
        "Dropped %d rows with blanks or '?'. New shape: %s", before_dropna - df.shape[0], df.shape
    )

    # Drop rows where SaleAmount is 0.00
    if "SaleAmount" in df.columns:
        df["SaleAmount"] = pd.to_numeric(df["SaleAmount"], errors="coerce").fillna(0.0)
        before_zero = df.shape[0]
        df = df[df["SaleAmount"] != 0.00]
        logger.info(
            "Removed %d rows with 0.00 SaleAmount. New shape: %s",
            before_zero - df.shape[0],
            df.shape,
        )

    # Standardize PaymentMethod
    if "PaymentMethod" in df.columns:
        df["PaymentMethod"] = df["PaymentMethod"].str.replace(" ", "").str.title()
        df["PaymentMethod"] = df["PaymentMethod"].replace(
            {
                "Creditcard": "Credit Card",
                "Paypal": "PayPal",
                "Giftcard": "GiftCard",
                "Bitcoin": pd.NA,  # Not a valid method
            }
        )
        before_payment = df.shape[0]
        df = df[df["PaymentMethod"].notna()]
        logger.info(
            "Removed %d rows with invalid PaymentMethod. New shape: %s",
            before_payment - df.shape[0],
            df.shape,
        )

    # Convert CampaignID to whole numbers
    if "CampaignID" in df.columns:
        df["CampaignID"] = pd.to_numeric(df["CampaignID"], errors="coerce").fillna(0).astype(int)
        logger.info("Converted CampaignID to whole numbers")

    # Drop rows with critical missing values
    required_cols = ["TransactionID", "SaleDate", "CustomerID", "ProductID", "SaleAmount"]
    before_required = df.shape[0]
    df.dropna(subset=required_cols, inplace=True)
    logger.info(
        "Dropped %d rows missing critical fields. Final shape: %s",
        before_required - df.shape[0],
        df.shape,
    )

    # OPTIONAL: Data profiling
    logger.info("Column datatypes:\n%s", df.dtypes)
    logger.info("Unique values per column:\n%s", df.nunique())

    return df


#####################################
# Define Main Function - The main entry point of the script
#####################################


def main() -> pd.DataFrame:
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
        f"{old} -> {new}"
        for old, new in zip(original_columns, df.columns, strict=True)
        if old != new
    ]
    if changed_columns:
        logger.info(f"Cleaned column names: {', '.join(changed_columns)}")

    # Apply cleaning logic
    df = clean_sales_data(df)

    # Remove duplicates
    before_dedup = df.shape[0]
    df = df.drop_duplicates(subset=["TransactionID"], keep="first")
    logger.info(
        "Removed %d duplicate transactions. New shape: %s",
        before_dedup - df.shape[0],
        df.shape,
    )

    # Handle missing values with appropriate strategies
    # For numeric columns, replace missing values with median
    numeric_cols = df.select_dtypes(include=["int64", "float64"]).columns
    for col in numeric_cols:
        if df[col].isna().any():
            median_value = df[col].median()
            df[col].fillna(median_value, inplace=True)
            logger.info(f"Filled {col} missing values with median: {median_value}")

    # For categorical columns, replace missing values with mode
    categorical_cols = df.select_dtypes(include=["object"]).columns
    for col in categorical_cols:
        if df[col].isna().any():
            mode_value = df[col].mode()[0]
            df[col].fillna(mode_value, inplace=True)
            logger.info(f"Filled {col} missing values with mode: {mode_value}")

    # Remove outliers using IQR method for numeric columns
    def remove_outliers(df: pd.DataFrame, column: str) -> pd.DataFrame:
        q1 = df[column].quantile(0.25)
        q3 = df[column].quantile(0.75)
        iqr = q3 - q1
        lower_bound = q1 - 1.5 * iqr
        upper_bound = q3 + 1.5 * iqr

        before_outliers = df.shape[0]
        df = df[(df[column] >= lower_bound) & (df[column] <= upper_bound)]
        removed = before_outliers - df.shape[0]
        if removed > 0:
            logger.info(
                f"Removed {removed} outliers from {column} (bounds: {lower_bound:.2f} to {upper_bound:.2f})"
            )
        return df

    # Apply outlier removal to numeric columns that should have bounds
    numeric_cols_for_outliers = ["SaleAmount"]  # Add other numeric columns as needed
    for col in numeric_cols_for_outliers:
        if col in df.columns:
            df = remove_outliers(df, col)

    logger.info("==================================")
    logger.info(f"Original shape: {original_shape}")
    logger.info(f"Cleaned shape:  {df.shape}")
    logger.info("==================================")
    logger.info("FINISHED prepare_sales_data.py")
    logger.info("==================================")

    # Save prepared data
    output_path = PREPARED_DATA_DIR / output_file
    df.to_csv(output_path, index=False)
    logger.info(f"Saved prepared data to: {output_path}")

    return df


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
