import numpy as np
import pandas as pd
import sqlite3
import pathlib
import sys

# For local imports, temporarily add project root to sys.path
# Note: this can be removed - our project uses a modern /src/ folder and __init__.py files
# To make local imports easier.
# Adjust the paths and code to fit with this updated organization.
# Questions: Ask them here in this project discussion and we can help.
PROJECT_ROOT = pathlib.Path(__file__).resolve().parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.append(str(PROJECT_ROOT))

# Constants
DW_DIR = pathlib.Path("data").joinpath("dw")
DB_PATH = DW_DIR.joinpath("smart_sales.db")
PREPARED_DATA_DIR = pathlib.Path("data").joinpath("prepared")


def create_schema(cursor: sqlite3.Cursor) -> None:
    """Create tables in the data warehouse if they don't exist."""
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS customer (
            customer_id INTEGER PRIMARY KEY,
            region TEXT,
            join_date TEXT,
            loyalty_points INTEGER,
            engagement_style TEXT
        )
    """)

    cursor.execute("""
        CREATE TABLE IF NOT EXISTS product (
            product_id INTEGER PRIMARY KEY,
            product_name TEXT,
            category TEXT,
            unit_price REAL
        )
    """)

    cursor.execute("""
        CREATE TABLE IF NOT EXISTS sale (
            sale_id INTEGER PRIMARY KEY,
            customer_id INTEGER,
            product_id INTEGER,
            store_id INTEGER,
            campaign_id INTEGER,
            sale_amount REAL,
            sale_date TEXT,
            discount_percent REAL,
            FOREIGN KEY (customer_id) REFERENCES customer (customer_id),
            FOREIGN KEY (product_id) REFERENCES product (product_id),
            FOREIGN KEY (store_id) REFERENCES store (store_id),
            FOREIGN KEY (campaign_id) REFERENCES campaign (campaign_id)
        )
    """)

    cursor.execute("""
    CREATE TABLE IF NOT EXISTS store (
        store_id INTEGER PRIMARY KEY,
        store_name TEXT,
        region TEXT
    )
    """)

    cursor.execute("""
        CREATE TABLE IF NOT EXISTS campaign (
            campaign_id INTEGER PRIMARY KEY,
            campaign_name TEXT,
            start_date TEXT,
            end_date TEXT
        )
    """)


def delete_existing_records(cursor: sqlite3.Cursor) -> None:
    """Delete all existing records from the customer, product, and sale tables."""
    cursor.execute("DELETE FROM customer")
    cursor.execute("DELETE FROM product")
    cursor.execute("DELETE FROM sale")
    cursor.execute("DELETE FROM store")
    cursor.execute("DELETE FROM campaign")

    cursor.execute("SELECT COUNT(*) FROM customer")
    print("Customer rows reamaining after deletion:", cursor.fetchone()[0])


def insert_customers(customers_df: pd.DataFrame, cursor: sqlite3.Cursor) -> None:
    """Insert customer data into the customer table."""
    customers_df.to_sql("customer", cursor.connection, if_exists="append", index=False)


def insert_products(products_df: pd.DataFrame, cursor: sqlite3.Cursor) -> None:
    """Insert product data into the product table."""
    products_df.to_sql("product", cursor.connection, if_exists="append", index=False)


def insert_sales(sales_df: pd.DataFrame, cursor: sqlite3.Cursor) -> None:
    """Insert sales data into the sales table."""
    sales_df.to_sql("sale", cursor.connection, if_exists="append", index=False)


def insert_mock_stores(cursor: sqlite3.Cursor) -> None:
    stores = [
        (401, "Los Angeles Plaza", "West"),
        (402, "Phoenix Outfitters", "South-West"),
        (403, "Downtown Seattle", "North"),
        (404, "New York Uptown", "East"),
    ]
    cursor.executemany("INSERT INTO store VALUES (?, ?, ?)", stores)


def insert_mock_campaigns(cursor: sqlite3.Cursor) -> None:
    campaigns = [
        (0, "Summer Sale", "2025-06-01", "2025-07-31"),
        (1, "Holiday Promo", "2025-11-01", "2025-12-31"),
        (2, "Back to School", "2025-08-01", "2025-09-15"),
        (3, "NeW Year Kickoff", "2025-01-01", "2025-01-31"),
    ]
    cursor.executemany("INSERT INTO campaign VALUES (?, ?, ?, ?)", campaigns)


def load_data_to_db() -> None:
    try:
        # Connect to SQLite – will create the file if it doesn't exist
        conn = sqlite3.connect(DB_PATH)
        cursor = conn.cursor()

        # Create schema and clear existing records
        create_schema(cursor)
        delete_existing_records(cursor)

        # Insert mock reference data
        insert_mock_stores(cursor)
        insert_mock_campaigns(cursor)

        # Load prepared data using pandas
        customers_df = pd.read_csv(PREPARED_DATA_DIR.joinpath("customers_prepared.csv"))
        # 🔍 Check for duplicate customer IDs
        dupes = customers_df[customers_df["customer_id"].duplicated(keep=False)]
        print("Duplicate rows:\n", dupes)

        print("Duplicate customer_ids:", customers_df["customer_id"].duplicated().sum())

        products_df = pd.read_csv(PREPARED_DATA_DIR.joinpath("products_prepared.csv"))
        sales_df = pd.read_csv(PREPARED_DATA_DIR.joinpath("sales_data_prepared.csv"))

        # Rename TransactionID to match the database schema
        sales_df.rename(columns={"transaction_id": "sale_id"}, inplace=True)

        # Optional: drop duplicates based on sale_id
        sales_df = sales_df.drop_duplicates(subset="sale_id", keep="first")

        # Randomize sale_date across a 6-month range
        sales_df["sale_date"] = pd.to_datetime("2025-01-01") + pd.to_timedelta(
            np.random.randint(0, 365, size=len(sales_df)), unit="D"
        )
        sales_df["sale_date"] = sales_df["sale_date"].dt.strftime("%Y-%m-%d")

        sale_columns = [
            "sale_id",
            "customer_id",
            "product_id",
            "store_id",
            "campaign_id",
            "sale_amount",
            "sale_date",
            "discount_percent",
        ]
        sales_df = sales_df[sale_columns]

        product_columns = ["product_id", "product_name", "category", "unit_price"]
        products_df = products_df[product_columns]

        customer_columns = [
            "customer_id",
            "region",
            "join_date",
            "loyalty_points",
            "engagement_style",
        ]
        customers_df = customers_df[customer_columns]

        # Insert data into the database
        insert_customers(customers_df, cursor)
        insert_products(products_df, cursor)
        insert_sales(sales_df, cursor)

        conn.commit()
    finally:
        if conn:
            conn.close()


if __name__ == "__main__":
    load_data_to_db()
