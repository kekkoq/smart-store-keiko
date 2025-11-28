import sqlite3

conn = sqlite3.connect("smart_sales.db")
cursor = conn.cursor()

# Get all table names
cursor.execute("SELECT name FROM sqlite_master WHERE type='table';")
tables = [row[0] for row in cursor.fetchall()]  # Extract just the name string

# Print columns for each table
for table in tables:
    cursor.execute(f'PRAGMA table_info("{table}");')
    columns = [row[1] for row in cursor.fetchall()]
    print(f"\nTable: {table}")
    for col in columns:
        print(f"  - {col}")
