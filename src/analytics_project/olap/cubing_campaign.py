"""Module 6: OLAP Goal Script (uses cubed results).

File: src/analytics_project/olap/goal_sales_by_day.py.

Module: analytics_project.olap.goal_sales_by_day

This script uses our precomputed cubed data set to get the information
we need to answer a specific business goal.

GOAL:

ACTION: This can help inform decisions about reducing operating hours
or focusing marketing efforts on less profitable days.



"""

import sqlite3
import pathlib
import pandas as pd
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
    """Ingest sales + dimension tables from SQLite data warehouse."""
    try:
        with sqlite3.connect(str(DB_PATH)) as conn:
            sales_df = pd.read_sql_query("SELECT * FROM sale", conn)
            store_df = pd.read_sql_query("SELECT * FROM store", conn)
            campaign_df = pd.read_sql_query("SELECT * FROM campaign", conn)
            product_df = pd.read_sql_query("SELECT * FROM product", conn)
            customer_df = pd.read_sql_query("SELECT * FROM customer", conn)

        # Merge all dimension tables into sales_df
        sales_df = (
            sales_df.merge(store_df, on="store_id", how="left")
            .merge(campaign_df, on="campaign_id", how="left")
            .merge(product_df, on="product_id", how="left")
            .merge(customer_df, on="customer_id", how="left")
        )

        # Normalize region column name if pandas renamed it
        if "region_y" in sales_df.columns:
            sales_df.rename(columns={"region_y": "region"}, inplace=True)
        elif "region_x" in sales_df.columns:
            sales_df.rename(columns={"region_x": "region"}, inplace=True)

        logger.info(f"Merged sales_df columns: {sales_df.columns.tolist()}")
        logger.info(
            "Sales + store + campaign + product + customer data successfully loaded from SQLite DW."
        )
        logger.info(
            f"Sample regions in sales_df: {sales_df['region'].dropna().unique().tolist()[:5]}"
        )
        return sales_df

    except Exception as e:
        logger.error(f"Error loading sales data from data warehouse: {e}")
        raise


def generate_column_names(
    dimensions: list[str], metrics: dict, include_sale_ids: bool
) -> list[str]:
    """Generate explicit column names for OLAP cube (dimensions + metrics + sale_ids if present)."""
    column_names = dimensions.copy()
    for metric, agg in metrics.items():
        column_names.append(f"{metric}_{agg}")
    if include_sale_ids:
        column_names.append("sale_ids")
    return column_names


def create_olap_cube(sales_df: pd.DataFrame, dimensions: list[str], metrics: dict) -> pd.DataFrame:
    try:
        # Ensure numeric types
        for col in ["sale_amount", "sale_count", "campaign_cost"]:
            if col in sales_df.columns:
                sales_df[col] = pd.to_numeric(sales_df[col], errors="coerce")

        grouped = sales_df.groupby(dimensions, dropna=False)
        cube = grouped.agg(metrics).reset_index()

        include_sale_ids = "sale_ids" in sales_df.columns
        if include_sale_ids:
            cube["sale_ids"] = grouped["sale_ids"].apply(list).reset_index(drop=True)

        explicit_columns = generate_column_names(dimensions, metrics, include_sale_ids)
        cube.columns = explicit_columns

        rename_map = {
            "campaign_cost_first": "campaign_cost",
            "sale_amount_sum": "sale_amount",
            "sale_id_sum": "sale_count",
        }
        cube.rename(columns=rename_map, inplace=True)

        # ROI metrics
        if "sale_amount" in cube.columns and "campaign_cost" in cube.columns:
            cube["monthly_cost"] = cube["campaign_cost"] / 12
            cube["roi_monthly"] = (cube["sale_amount"] - cube["monthly_cost"]) / cube[
                "monthly_cost"
            ].replace(0, pd.NA)
            cube["cumulative_sales"] = cube.groupby(["Year", "campaign_name"], dropna=False)[
                "sale_amount"
            ].cumsum()
            cube["roi_cumulative"] = (cube["cumulative_sales"] - cube["campaign_cost"]) / cube[
                "campaign_cost"
            ].replace(0, pd.NA)
            cube["roi"] = (cube["sale_amount"] - cube["campaign_cost"]) / cube[
                "campaign_cost"
            ].replace(0, pd.NA)

        logger.info(f"Cube columns after rename: {cube.columns.tolist()}")
        logger.info(f"OLAP cube created with dimensions: {dimensions}")
        return cube

    except Exception as e:
        logger.error(f"Error creating OLAP cube: {e}")
        raise


def write_cube_to_csv(cube: pd.DataFrame, output_path: pathlib.Path) -> None:
    """Write the OLAP cube to a CSV file."""
    try:
        logger.info(
            f"Attempting to write cube with shape: {cube.shape} and columns: {cube.columns.tolist()}"
        )
        output_path.parent.mkdir(parents=True, exist_ok=True)
        cube.to_csv(output_path, index=False)
        logger.info(f"OLAP cube saved to {output_path}.")
    except Exception as e:
        logger.error(f"Error saving OLAP cube to CSV file: {e}")
        raise
    finally:
        logger.info(f"Write complete? File exists: {output_path.exists()}")


def main():
    logger.info("Starting OLAP Cubing process...")

    sales_df = ingest_sales_data_from_dw()
    if sales_df.empty:
        logger.warning("WARNING: The sales table is empty. OLAP cube will contain only headers.")
        return

    sales_df["sale_date"] = pd.to_datetime(sales_df["sale_date"], errors="coerce")
    sales_df["Year"] = sales_df["sale_date"].dt.year
    sales_df["Month"] = sales_df["sale_date"].dt.month

    # Replace store_name with region
    dimensions = ["Year", "Month", "region", "campaign_name", "category"]
    metrics = {"sale_amount": "sum", "sale_id": "count", "campaign_cost": "first"}

    olap_cube = create_olap_cube(sales_df, dimensions, metrics)
    write_cube_to_csv(olap_cube, CUBED_FILE)

    logger.info("OLAP Cubing process completed successfully.")
    logger.info(f"Please see outputs in {OLAP_OUTPUT_DIR}")
    logger.info(f"Expected output: {CUBED_FILE}")
    logger.info(f"File exists after write: {CUBED_FILE.exists()}")


if __name__ == "__main__":
    main()
