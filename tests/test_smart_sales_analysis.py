
"""
Query the smart_sales SQLite database and return aggregated sales results.

This function:
- Connects to the SQLite database at ``data/dw/smart_sales.db``.
- Joins the ``sale`` and ``store`` tables.
- Computes ``SUM(sale_amount)`` per store and region.
- Returns the results as a pandas DataFrame sorted by region and total sales.

Returns
-------
pd.DataFrame
    Aggregated sales results with columns:
    - region
    - store_name
    - total_sales
"""


import sqlite3
import pandas as pd

# Connect to the database
conn = sqlite3.connect("data/dw/smart_sales.db")  # adjust path if needed

# Write your SQL query
query = """
SELECT
    st.region,
    st.store_name,
    SUM(sa.sale_amount) AS total_sales
FROM
    sale AS sa
JOIN
    store AS st
    ON sa.store_id = st.store_id
GROUP BY
    st.region, st.store_id
ORDER BY
    st.region, total_sales DESC
"""

# Run the query and load into a DataFrame
df = pd.read_sql_query(query, conn)

print(df)
