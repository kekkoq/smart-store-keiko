import pathlib
import sqlite3
import sys

import numpy as np
import pandas as pd

# For local imports, temporarily add project root to sys.path
# Note: this can be removed - our project uses a modern /src/ folder and __init__.py files
# To make local imports easier.

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

    cursor.execute("DROP TABLE IF EXISTS campaign")
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS campaign (
            campaign_id INTEGER PRIMARY KEY,
            campaign_name TEXT,
            campaign_cost REAL
        )
    """)


def delete_existing_records(cursor: sqlite3.Cursor) -> None:
    """Delete all existing records from customer, product, sale, store, and campaign tables."""
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
        (0, "Rewards Program", 350000.0),
        (1, "Discount Bundle", 300000.0),
        (2, "Premium Upsell", 350000.0),
        (3, "Referral Incentives", 300000.0),
    ]
    cursor.executemany("INSERT INTO campaign VALUES (?, ?, ?)", campaigns)


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

        mask = customers_df["engagement_style"] == "Unknown"
        customers_df.loc[mask, "engagement_style"] = np.random.choice(
            ["Desktop", "Mobile", "InStore"], size=mask.sum()
        )

        # Insert placeholder rows for missing customer_ids
        placeholder_customers = pd.DataFrame(
            [
                {
                    "customer_id": 1000,
                    "region": "South-West",
                    "join_date": "2021-11-01",
                    "loyalty_points": 719,
                    "engagement_style": "Mobile",
                },
                {
                    "customer_id": 1140,
                    "region": "Central",
                    "join_date": "2023-06-15",
                    "loyalty_points": 5099,
                    "engagement_style": "InStore",
                },
                {
                    "customer_id": 1168,
                    "region": "East",
                    "join_date": "2024-10-31",
                    "loyalty_points": 7138,
                    "engagement_style": "Desktop",
                },
            ]
        )

        customers_df = pd.concat([customers_df, placeholder_customers], ignore_index=True)

        # Align columns to match the customer table schema
        customer_columns = [
            "customer_id",
            "region",
            "join_date",
            "loyalty_points",
            "engagement_style",
        ]
        customers_df = customers_df[customer_columns]

        # 🔍 Check for duplicate customer IDs
        dupes = customers_df[customers_df["customer_id"].duplicated(keep=False)]
        print("Duplicate rows:\n", dupes)

        print("Duplicate customer_ids:", customers_df["customer_id"].duplicated().sum())

        products_df = pd.read_csv(PREPARED_DATA_DIR.joinpath("products_prepared.csv"))
        # Insert placeholder rows for missing product_ids
        placeholder_products = pd.DataFrame(
            [
                {
                    "product_id": 2000,
                    "product_name": "Electronics-Home",
                    "category": "Electronics",
                    "unit_price": 963.31,
                },
                {
                    "product_id": 2083,
                    "product_name": "Clothing-Cut",
                    "category": "Home",
                    "unit_price": 768.16,
                },
            ]
        )

        products_df = pd.concat([products_df, placeholder_products], ignore_index=True)

        # Align columns to match the product table schema
        product_columns = ["product_id", "product_name", "category", "unit_price"]
        products_df = products_df[product_columns]

        # Continue with sales load

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

        # Align columns to match the product table schema
        product_columns = ["product_id", "product_name", "category", "unit_price"]
        products_df = products_df[product_columns]

        # Step 2: Apply product name corrections
        product_name_corrections = {2048: "Air Purifier"}
        for pid, new_name in product_name_corrections.items():
            products_df.loc[products_df['product_id'] == pid, 'product_name'] = new_name

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
