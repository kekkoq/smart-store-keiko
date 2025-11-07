"""prepare_products_data.py.

This script reads products data from the data/raw folder, cleans the data,
and writes the cleaned version to the data/prepared folder.

Steps:
- Remove duplicates
- Handle missing values
- Remove outliers
- Save cleaned data
"""

# ================================

# Import from Python Standard Library
import pathlib
import sys

# Add project root to sys.path (so local imports work)
sys.path.append(str(pathlib.Path(__file__).resolve().parent.parent.parent))

# Import from external packages
import pandas as pd

# Import local modules
from analytics_project.utils.logger import logger

# ================================
# Path Setup
# ================================
SCRIPTS_DIR = pathlib.Path(__file__).resolve().parent
PROJECT_ROOT = SCRIPTS_DIR.parent.parent.parent
DATA_DIR = PROJECT_ROOT / "data"
RAW_DATA_DIR = DATA_DIR / "raw"
PREPARED_DATA_DIR = DATA_DIR / "prepared"

print(f"RAW_DATA_DIR resolved to: {RAW_DATA_DIR}")

# Ensure directories exist
DATA_DIR.mkdir(exist_ok=True)
RAW_DATA_DIR.mkdir(exist_ok=True)
PREPARED_DATA_DIR.mkdir(exist_ok=True)

# ================================
# Functions
# ================================


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

    if "StockLevel" in df.columns:
        median_points = df["StockLevel"].median()
        df.loc[:, "StockLevel"] = df["StockLevel"].fillna(median_points)

    if "SupplierTier" in df.columns and not df["SupplierTier"].mode().empty:
        mode_style = df["SupplierTier"].mode()[0]
        df.loc[:, "SupplierTier"] = df["SupplierTier"].fillna(mode_style)

    missing_after = df.isna().sum().sum()
    logger.info(f"Total missing values after handling: {missing_after}")
    return df


def clean_values(df: pd.DataFrame) -> pd.DataFrame:
    logger.info(f"FUNCTION START: clean values with shape: {df.shape}")

    # Remove negative StockLevel
    if "StockLevel" in df.columns:
        negative_count = (df["StockLevel"] < 0).sum()
        df = df[df["StockLevel"] >= 0].copy()
        logger.info(f"Removed {negative_count} rows with negative StockLevel")

        df["StockLevel"] = df["StockLevel"].astype(int)
        logger.info("Converted StockLevel to integer type")

    # Standardize SupplierTier values
    if "SupplierTier" in df.columns:
        original_tiers = df["SupplierTier"].dropna().unique().tolist()

        # Normalize whitespace and casing
        df["SupplierTier"] = df["SupplierTier"].astype(str).str.strip().str.title()

        # Replace 'Unknown' with 'Standard'
        df["SupplierTier"] = df["SupplierTier"].replace({"Unknown": "Standard"})

        # Validate against known tiers
        tier_map = {
            "Basic": "Basic",
            "Preferred": "Preferred",
            "Premium": "Premium",
            "Standard": "Standard",  # Include Standard as valid tier
        }
        df["SupplierTier"] = df["SupplierTier"].map(tier_map).fillna("Standard")

        updated_tiers = df["SupplierTier"].unique().tolist()
        logger.info(f"Standardized SupplierTier from {original_tiers} to {updated_tiers}")

    return df


def remove_outliers(df: pd.DataFrame) -> pd.DataFrame:
    logger.info(f"Removing outliers from shape: {df.shape}")
    if "StockLevel" in df.columns:
        mean = df["StockLevel"].mean()
        std = df["StockLevel"].std()
        lower = mean - 3 * std
        upper = mean + 3 * std
        original_count = len(df)
        df = df[df["StockLevel"].between(lower, upper)]
        removed = original_count - len(df)
        logger.info(f"Outliers removed: {removed}")
    return df


# ================================
# Main
# ================================


def main() -> None:
    logger.info("=== STARTING prepare_products_data.py ===")
    input_file = "products_data.csv"
    output_file = "products_prepared.csv"

    df = read_raw_data(input_file)
    print(f"File loaded: {not df.empty}")
    print(f"volumns: {df.columns.tolist()}")
    print(df.isna().sum())

    if not df.empty and df.columns.dtype == "object":
        df.columns = df.columns.str.strip()

    df = remove_duplicates(df)
    df = handle_missing_values(df)
    df = clean_values(df)
    df = remove_outliers(df)

    save_prepared_data(df, output_file)
    logger.info("=== FINISHED prepare_products_data.py ===")


# ================================
# Entry Point
# ================================
if __name__ == "__main__":
    main()
