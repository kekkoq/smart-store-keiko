"""Module 6: OLAP and Cubing Script.

File: src/analytics_project/olap/cubing.py.

Module: analytics_project.olap.cubing

A cube is a precomputed, multidimensional structure
where data is aggregated across all possible
combinations of selected dimensions
(e.g., DayOfWeek, ProductID).

Purpose: It allows for fast querying and analysis
across many dimensions without needing to
compute aggregations on the fly.
Structure: The result is stored as a
multidimensional dataset that can be
queried with SQL-like syntax
or visualized in BI tools.


This example script handles OLAP cubing with Python.
It ingests data from a data warehouse,
performs aggregations for multiple dimensions,
and creates OLAP cubes.
The cubes are saved as CSV files for further analysis.
Cubes might also be kept in Power BI, Snowflake, Looker, or another tool.

Input Data:

- A fact table (sales): Includes sale_date, product_id, customer_id, sale_amount, etc.
- Dimension tables: Define attributes like products, customers, and more

Output Cube:

- The cube contains precomputed totals, averages, counts,
and other metrics for all combinations of DayOfWeek, ProductID, and CustomerID.

AFTER CREATION, we can Query the Cube:

- Slice: e.g., Extract sales for a specific customer (or specific store or region, depending on your data).
- Dice: e.g., Filter sales for specific combinations of ProductID and other (e.g., store, region, campaign, depending on your data)
- Drill-down: e.g., Aggregate sales by DayOfWeek (within a specific store or region, depending on your data)

IMPORTANT: The OLAP cubing script needs to align
with your data warehouse (DW) structure and
the etl_to_dw.py script that defines your database schema.

THIS EXAMPLE INPUTS DIMENSION AND FACT TABLES:

This example assumes a simple data warehouse structure with one fact table (`sale`)
and two dimension tables (`product` and `customer`). These tables collectively enable
multidimensional analysis using OLAP cubing.


THIS EXAMPLE OUTPUTS:

This example assumes a cube data set with the following column names (yours will differ).
DayOfWeek,product_id,customer_id,sale_amount_usd_sum,sale_id_count,sale_ids
Friday,101,1001,6344.96,1,[582]
etc.

"""

import pathlib
import sqlite3

import pandas as pd
import matplotlib.pyplot as plt

from analytics_project.utils.logger import init_logger, logger

init_logger()

# Global paths
THIS_DIR = pathlib.Path(__file__).resolve().parent
PACKAGE_DIR = THIS_DIR.parent
SRC_DIR = PACKAGE_DIR.parent
PROJECT_ROOT_DIR = SRC_DIR.parent

DATA_DIR = PROJECT_ROOT_DIR / "data"
WAREHOUSE_DIR = DATA_DIR / "dw"
DB_PATH = WAREHOUSE_DIR / "smart_sales.db"

OLAP_OUTPUT_DIR = DATA_DIR / "olap_cubing_outputs"
OLAP_OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
CUBED_FILE = OLAP_OUTPUT_DIR / "campaign_effectiveness.csv"

RESULTS_OUTPUT_DIR = DATA_DIR / "results"
RESULTS_OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

logger.info(f"THIS_DIR:            {THIS_DIR}")
logger.info(f"PACKAGE_DIR:         {PACKAGE_DIR}")
logger.info(f"SRC_DIR:             {SRC_DIR}")
logger.info(f"PROJECT_ROOT_DIR:    {PROJECT_ROOT_DIR}")
logger.info(f"DATA_DIR:            {DATA_DIR}")
logger.info(f"WAREHOUSE_DIR:       {WAREHOUSE_DIR}")
logger.info(f"DB_PATH:             {DB_PATH}")
logger.info(f"OLAP_OUTPUT_DIR:     {OLAP_OUTPUT_DIR}")
logger.info(f"RESULTS_OUTPUT_DIR:  {RESULTS_OUTPUT_DIR}")


def ingest_sales_data_from_dw() -> pd.DataFrame:
    """Ingest data from SQLite data warehouse."""
    try:
        conn = sqlite3.connect(DB_PATH)
        sales_df = pd.read_sql_query("SELECT * FROM sale", conn)
        product_df = pd.read_sql_query("SELECT * FROM product", conn)
        store_df = pd.read_sql_query("SELECT * FROM store", conn)
        conn.close()

        # Merge store and product into sales
        sales_df = sales_df.merge(store_df, on="store_id", how="left").merge(
            product_df, on="product_id", how="left"
        )

        logger.info("Sales + store + product data successfully loaded from SQLite DW.")
        return sales_df
    except Exception as e:
        logger.error(f"Error loading sales data from data warehouse: {e}")
        raise


def get_top3_products_per_campaign(sales_df: pd.DataFrame) -> pd.DataFrame:
    """Return top 3 products per campaign based on row counts (number of sales)."""
    grouped = (
        sales_df.groupby(['store_name', 'product_name'])
        .size()  # counts rows
        .reset_index(name='sale_count')
    )
    grouped['rank'] = grouped.groupby('store_name')['sale_count'].rank(
        method='first', ascending=False
    )
    return grouped[grouped['rank'] <= 3]


def alias_products(top3_df: pd.DataFrame, alias_map: dict) -> pd.DataFrame:
    """Apply human-friendly aliases to product names."""
    top3_df['product_alias'] = (
        top3_df['product_name'].map(alias_map).fillna(top3_df['product_name'])
    )
    return top3_df


def plot_top3_products(top3_df: pd.DataFrame):
    """Visualize top 3 products per store."""
    fig, ax = plt.subplots(figsize=(12, 6))
    for store, subset in top3_df.groupby('store_name'):
        # fallback to product_name if alias not present
        labels = subset['product_alias'] if 'product_alias' in subset else subset['product_name']
        ax.bar(labels, subset['sale_count'], label=store)

    plt.title("Top 3 Products per Store")
    plt.ylabel("Sales Count")
    plt.xticks(rotation=45, ha='right')
    plt.legend(title="Store")
    plt.tight_layout()
    plt.show()


def main():
    sales_df = ingest_sales_data_from_dw()

    top3_df = get_top3_products_per_campaign(sales_df)

    # Optional aliasing
    alias_map = {
        "Electronics-Assume": "Men's Jacket",
        "Office-Raise": "Women's Skirt",
        "Office-Receive": "Tuxedo Suit",
        "Office-Term": "Small Sofa",
        "Office-Year": "Gas Range",
        "Home-Century": "Blender",
        "Office-Soon": "Wedding Dress",
        "Clothing-Lawyer": "Air Flyer",
        "Home-He": "Desktop Computer",
        "Electronics-Candidate": "Earbuds",
        "Clothing-Garden": "Lawn Mower",
    }

    top3_df = alias_products(top3_df, alias_map)

    # Sort by store and descending sale count
    top3_df = top3_df.sort_values(by=['store_name', 'sale_count'], ascending=[True, False])

    plot_top3_products(top3_df)


if __name__ == "__main__":
    main()
