import sqlite3
import pandas as pd

# Connect to the existing database
conn = sqlite3.connect("data/dw/smart_sales.db")
cursor = conn.cursor()

# Drop and recreate the sale table
cursor.execute("DROP TABLE IF EXISTS sale;")
cursor.execute("""
CREATE TABLE sale (
    transaction_id INTEGER PRIMARY KEY,
    sale_date TEXT,
    customer_id INTEGER,
    product_id INTEGER,
    store_id INTEGER,
    campaign_id INTEGER,
    sale_amount REAL,
    discount_percent REAL,
    payment_method TEXT
);
""")

# Load cleaned data
df = pd.read_csv("data/prepared/customers_prepared.csv")
df.to_sql("customer", conn, if_exists="append", index=False)

conn.commit()
conn.close()
print(f"[Reset] Loaded {len(df)} rows into fresh 'sale' table.")
