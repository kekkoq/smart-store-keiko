import sqlite3
import os
import pandas as pd

# Path to your database
db_path = "C:/Repos/smart-store-keiko/data/dw/smart_sales.db"

# Check if the file exists and is non-empty
if not os.path.exists(db_path):
    print(f"❌ Database file not found at: {db_path}")
elif os.path.getsize(db_path) == 0:
    print(f"⚠️ Database file exists but is empty (0 KB): {db_path}")
else:
    try:
        conn = sqlite3.connect(db_path)
        cursor = conn.cursor()

        # List all tables and views
        cursor.execute("""
            SELECT name, type
            FROM sqlite_master
            WHERE type IN ('table', 'view')
            ORDER BY type, name;
        """)
        results = cursor.fetchall()

        print(f"\nInspecting: {db_path}")
        if results:
            print("✅ Database is intact. Found the following objects:")
            for name, obj_type in results:
                print(f" - {obj_type.upper()}: {name}")

            # Inspect columns in the 'campaign' table
            print("\nColumns in 'campaign' table:")
            cursor.execute("PRAGMA table_info(campaign);")
            columns = cursor.fetchall()
            for col in columns:
                print(f" - {col[1]} ({col[2]})")

            # Print all rows in 'campaign' table
            print("\nRows in 'campaign' table:")
            cursor.execute("SELECT * FROM campaign;")
            rows = cursor.fetchall()
            for row in rows:
                print(row)

            # Optional: pretty-print as a DataFrame
            df = pd.DataFrame(rows, columns=[col[1] for col in columns])
            print("\nCampaign table as DataFrame:")
            print(df)

        else:
            print("⚠️ File exists but contains no tables or views.")

    except sqlite3.Error as e:
        print(f"❌ SQLite error: {e}")
    finally:
        conn.close()
