import os
import pandas as pd
from pandasql import sqldf

# Import the actual logger object and initializer
from analytics_project.utils.logger import logger, init_logger

# Initialize logging once at the start
init_logger()

PROJECT_ROOT_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..', '..'))
DATA_DIR = os.path.join(PROJECT_ROOT_DIR, 'data', 'olap_cubing_outputs')
csv_path = os.path.join(DATA_DIR, 'campaign_effectiveness.csv')

logger.info(f"Resolved CSV path: {csv_path}")

# Sanity check before reading
if not os.path.exists(csv_path):
    logger.error(f"CSV file not found at {csv_path}")
else:
    sales_df = pd.read_csv(csv_path)

    pysqldf = lambda q: sqldf(q, {"sales_df": sales_df})

    query = """
   SELECT
        SUM(sale_amount) AS total_sales,
        SUM(campaign_cost_per_campaign) AS total_cost,
        (SUM(sale_amount) - SUM(campaign_cost_per_campaign)) / SUM(campaign_cost_per_campaign) AS total_roi
    FROM (
        SELECT
            campaign_name,
            SUM(sale_amount) AS sale_amount,
            MAX(campaign_cost) AS campaign_cost_per_campaign
            FROM sales_df
            GROUP BY campaign_name
        ) AS campaign_summary;
    """
    results = pysqldf(query)
    print(results)
