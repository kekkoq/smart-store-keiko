import os
import pandas as pd
from pandasql import sqldf

DATA_DIR = os.path.join(os.path.dirname(__file__), '..', 'data', 'olap_cubing_outputs')
csv_path = os.path.join(DATA_DIR, 'campaign_effectiveness.csv')

sales_df = pd.read_csv(csv_path)

pysqldf = lambda q: sqldf(q, {"sales_df": sales_df})

query = """
SELECT month,
       campaign_name,
       SUM(sale_amount) AS monthly_sales,
       MAX(cumulative_sales) AS ytd_sales,
       MAX(campaign_cost) AS campaign_cost,
       MAX(roi_cumulative) AS cumulative_roi
FROM sales_df
Group by month,campaign_name
ORDER BY campaign_name, month;
"""

results = pysqldf(query)
print(results)
