import os
import pandas as pd
from pandasql import sqldf
from analytics_project.utils.logger import logger, init_logger

init_logger()

PROJECT_ROOT_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
DATA_DIR = os.path.join(PROJECT_ROOT_DIR, 'data', 'olap_cubing_outputs')
csv_path = os.path.join(DATA_DIR, 'campaign_effectiveness.csv')

logger.info(f"Resolved CSV path: {csv_path}")

if not os.path.exists(csv_path):
    logger.error(f"CSV file not found at {csv_path}")
else:
    sales_df = pd.read_csv(csv_path)

    # ✅ Convert column types
    sales_df["unit_price"] = pd.to_numeric(sales_df["unit_price"], errors="coerce")
    sales_df["stock_level"] = pd.to_numeric(sales_df["stock_level"], errors="coerce").astype("Int64")
    sales_df["cumulative_roi"] = pd.to_numeric(sales_df["cumulative_roi"], errors="coerce")

    # ✅ Example query
    pysqldf = lambda q: sqldf(q, {"sales_df": sales_df})
    query = """
    SELECT campaign_name, AVG(cumulative_roi) AS avg_roi
    FROM sales_df
    GROUP BY campaign_name
    ORDER BY avg_roi DESC
    """
    results = pysqldf(query)
    print(results)
