"""run_my_cleaning.py.

Small, focused script to clean the customers dataset using the project's
DataScrubber pipeline. This file is intentionally lightweight and safe to
run from the repository root.
"""

import logging
from pathlib import Path
import sys
from typing import cast

import pandas as pd


def configure_path() -> Path:
    """Ensure `src/` is on sys.path and return the repository root.

    Returns
    -------
    Path
        Path to repository root (two levels up from this script).
    """
    repo_root = Path(__file__).resolve().parent.parent
    src_dir = repo_root / "src"
    if src_dir.exists():
        sys.path.insert(0, str(src_dir.resolve()))
    else:
        logging.getLogger(__name__).debug("src/ not found at %s", src_dir)
    return repo_root


def get_logger() -> logging.Logger:
    """Return the project logger if available, else configure a simple one."""
    try:
        from analytics_project.utils.logger import logger  # type: ignore

        return cast("logging.Logger", logger)
    except Exception:
        logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s")
        return logging.getLogger("run_cleaning")


def clean_customers(input_path: Path | None = None, output_path: Path | None = None) -> int:
    """Load, clean, and save the customers dataset.

    Parameters
    ----------
    input_path : Path, optional
        CSV file to read. If not provided, defaults to data/raw/customers.csv
    output_path : Path, optional
        File to write cleaned output. If not provided, defaults to
        data/cleaned/customers_cleaned.csv

    Returns
    -------
    int
        Exit code (0 success, 1 failure)
    """
    repo_root = configure_path()
    logger = get_logger()

    raw_dir = repo_root / "data" / "raw"
    cleaned_dir = repo_root / "data" / "cleaned"
    cleaned_dir.mkdir(parents=True, exist_ok=True)

    if input_path is None:
        input_path = raw_dir / "customers.csv"
    if output_path is None:
        output_path = cleaned_dir / "customers_cleaned.csv"

    logger.info("Loading %s", input_path)
    if not input_path.exists():
        logger.error("Input file not found: %s", input_path)
        return 1

    try:
        df = pd.read_csv(input_path)
    except Exception as exc:
        logger.exception("Failed to read %s: %s", input_path, exc)
        return 1

    # Import DataScrubber after src was added to sys.path
    try:
        from analytics_project.data_preparation.data_scrubber import DataScrubber  # type: ignore
    except Exception as exc:
        logger.exception("Failed to import DataScrubber: %s", exc)
        return 1

    scrubber = DataScrubber(df)

    # Apply basic cleaning steps
    scrubber.standardize_column_names()
    scrubber.remove_duplicate_records()
    scrubber.handle_missing_data(fill_value="N/A")
    scrubber.rename_columns({"cust_id": "customer_id", "cust_name": "customer_name"})
    scrubber.format_column_strings("customer_name", case="lower")

    # Inspect results
    try:
        info, summary = scrubber.inspect()
        logger.info("Customers dataset info:\n%s\n%s", info, summary)
    except Exception:
        logger.debug("Inspection failed; continuing to save output")

    # Save output
    try:
        logger.info("Saving cleaned dataset to %s", output_path)
        scrubber.df.to_csv(output_path, index=False)
    except Exception as exc:
        logger.exception("Failed to save cleaned dataset: %s", exc)
        return 1

    logger.info("Cleaning completed successfully")
    return 0


def main() -> int:
    """Run the customers cleaning flow and return an OS-style exit code.

    Returns
    -------
    int
        0 on success, non-zero on failure.
    """
    return clean_customers()


if __name__ == "__main__":
    raise SystemExit(main())
