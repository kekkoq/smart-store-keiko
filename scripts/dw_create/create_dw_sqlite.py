"""scripts/dw_create/create_dw_sqlite.py.

This script creates the SQLite data warehouse using .sql files.

File locations:
- data/prepared         : cleaned and prepared CSVs (used later in ETL)
- dw/                   : data warehouse folder
- dw/smart_sales.sqlite : SQLite database file
- sql/dw_create         : folder for .sql files used to create tables

Notes:
- SQLite does not support a native DATE type.
  Dates should be stored as TEXT in ISO format: "YYYY-MM-DD".

Complete the TODOs to create and populate a SQLite data warehouse.
"""

# import from Python standard libraries
import pathlib
import sys
import sqlite3

# import from external libraries
import pandas as pd

# Add src/analytics_project to sys.path for local imports
sys.path.append(
    str(pathlib.Path(__file__).resolve().parent.parent.parent / "src" / "analytics_project")
)

# import from local modules
from utils.logger import logger

# === Path Constants ===

# Python script directories
SCRIPTS_DW_CREATE_DIR = pathlib.Path(__file__).resolve().parent
SCRIPTS_DIR = SCRIPTS_DW_CREATE_DIR.parent

# Project root directory
PROJECT_ROOT = SCRIPTS_DIR.parent

# Data directories
DATA_DIR = PROJECT_ROOT / "data"
DATA_PREPARED_DIR = DATA_DIR / "prepared"

# SQL directories
SQL_DIR = PROJECT_ROOT / "sql"
SQL_DW_CREATE_DIR = SQL_DIR / "dw_create"

# Output DW directory
DW_DIR = PROJECT_ROOT / "dw"

# Output DW SQLite database path
DW_PATH = DW_DIR / "smart_sales.sqlite"

# Ensure necessary directories exist
DW_DIR.mkdir(parents=True, exist_ok=True)
SQL_DW_CREATE_DIR.mkdir(parents=True, exist_ok=True)

# === Functions ===


def run_sql_file(conn, file_path: pathlib.Path) -> None:
    """Run a SQL script file using the given connection."""
    logger.info(f"RUN: {file_path}")
    try:
        with open(file_path, "r", encoding="utf-8") as f:
            sql = f.read()
            conn.executescript(sql)
    except Exception as e:
        logger.error(f"Error running {file_path.name}: {e}")
        raise


def create_dw() -> None:
    """Create the data warehouse schema using SQLite."""
    logger.info("Creating data warehouse using SQLite...")

    # Check if required SQL files exist
    sql_files = list(SQL_DW_CREATE_DIR.glob("*.sql"))
    if not sql_files:
        logger.error(
            f"No .sql files in {SQL_DW_CREATE_DIR}. Please add schema files before running."
        )
        exit(22)

    conn = None
    try:
        conn = sqlite3.connect(DW_PATH)

        # Run SQL scripts for creating tables
        # Use version 90 for sales table
        run_sql_file(conn, SQL_DW_CREATE_DIR / "00_drop_all_tables.sql")
        run_sql_file(conn, SQL_DW_CREATE_DIR / "10_create_customers.sql")
        run_sql_file(conn, SQL_DW_CREATE_DIR / "00_drop_all_tables.sql")
        run_sql_file(conn, SQL_DW_CREATE_DIR / "20_create_products.sql")
        run_sql_file(conn, SQL_DW_CREATE_DIR / "90_create_sales.sql")

        logger.info("Data warehouse schema created successfully.")

    except sqlite3.Error as e:
        logger.error(f"SQLite error: {e}")
        exit(23)
    except Exception as e:
        logger.error(f"Unexpected error: {e}")
        exit(24)
    finally:
        if conn:
            conn.close()


def populate_dw() -> None:
    """Populate the SQLite data warehouse with cleaned data."""
    logger.info("Populating SQLite data warehouse from prepared CSVs...")

    try:
        conn = sqlite3.connect(DW_PATH)

        # --- Load and insert customers ---
        # TODO: Add your new columns
        # TODO: Change as needed to reflect YOUR data and file names, etc.

        df_customers = pd.read_csv(DATA_PREPARED_DIR / "customers_prepared.csv")

        df_customers.to_sql("customers", conn, if_exists="append", index=False)
        logger.info(f"Inserted {len(df_customers)} rows into customers table.")

        # --- Load and insert products ---
        # TODO: Add your new columns
        # TODO: Change as needed to reflect YOUR data and file names, etc.

        df_products = pd.read_csv(DATA_PREPARED_DIR / "products_prepared.csv")

        df_products.to_sql("products", conn, if_exists="append", index=False)
        logger.info(f"Inserted {len(df_products)} rows into products table.")

        # --- Load and insert sales ---
        # TODO: Add your new columns
        # TODO: Change as needed to reflect YOUR data and file names, etc.

        df_sales = pd.read_csv(DATA_PREPARED_DIR / "sales_prepared.csv")

        # TODO: Decide if you want to drop any unused columns in any of the tables
        # Example of how to drop columns:

        drop_sales_columns = ["transaction_id", "payment_method"]
        df_sales = df_sales.drop(
            columns=[col for col in drop_sales_columns if col in df_sales.columns]
        )

        drop_customer_columns = ["name"]
        df_customers = df_customers.drop(
            columns=[col for col in drop_customer_columns if col in df_customers.columns]
        )

        drop_product_columns = ["stock_level", "supplier_tier"]
        df_products = df_products.drop(
            columns=[col for col in drop_product_columns if col in df_products.columns]
        )

        df_sales.to_sql("sales", conn, if_exists="append", index=False)
        logger.info(f"Inserted {len(df_sales)} rows into sales table.")

        logger.info("All tables populated successfully.")

    except Exception as e:
        logger.error(f"Error populating SQLite data warehouse: {e}")
        exit(1)
    finally:
        if conn:
            conn.close()


def main() -> None:
    """Run the warehouse creation process."""
    logger.info("========================================")
    logger.info("Starting: create_dw_sqlite.py")
    logger.info("========================================")
    logger.info(f"Root:   {PROJECT_ROOT}")
    logger.info(f"Scripts:{SCRIPTS_DW_CREATE_DIR}")
    logger.info(f"SQL:    {SQL_DW_CREATE_DIR}")
    logger.info(f"Input:  {DATA_PREPARED_DIR}")
    logger.info(f"Output: {DW_PATH}")
    create_dw()
    populate_dw()
    logger.info("========================================")
    logger.info("Finished: create_dw_sqlite.py")
    logger.info("========================================")


if __name__ == "__main__":
    main()
