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

# Display results
print(df)
