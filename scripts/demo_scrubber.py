"""Demo script for cleaning raw datasets using the DataScrubber pipeline.

This module sets up input/output paths (using pathlib), ensures required
directories exist, and invokes the `DataScrubber` from the project. It
logs progress and continues to the next dataset if a file or processing
step fails.
"""

import logging
from pathlib import Path
import sys
from typing import cast

import pandas as pd


def configure_path() -> Path:
    """Return the repository root path and ensure `src` is on sys.path.

    The repository root is considered to be two levels up from this
    script (scripts/ -> repo root). We add repo/src to sys.path so local
    imports like `analytics_project...` work when running this script
    directly.
    """
    repo_root = Path(__file__).resolve().parent.parent
    src_path = repo_root / "src"
    if src_path.exists():
        sys.path.insert(0, str(src_path.resolve()))
    else:
        # Still continue: some test runs may put packages on PYTHONPATH
        logging.getLogger(__name__).warning("Expected src/ at %s not found", src_path)
    return repo_root


def get_logger() -> logging.Logger:
    """Configure and return a logger for the script.

    Prefer the project's logger if available; otherwise fall back to a
    simple logger configured here.
    """
    try:
        # Attempt to import the project's logger (requires src on sys.path)
        from analytics_project.utils.logger import logger  # type: ignore

        # Cast to logging.Logger to satisfy type checkers (project logger should
        # behave like a standard logging.Logger)
        return cast("logging.Logger", logger)
    except Exception:
        logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s")
        return logging.getLogger("demo_scrubber")


def process_datasets(repo_root: Path, logger: logging.Logger) -> None:
    """Process the configured datasets (read, scrub, save).

    Parameters
    ----------
    repo_root : Path
        Path to the repository root (used to build data paths).
    logger : logging.Logger
        Logger to use for messages.
    """
    # Ensure cleaned directory exists
    cleaned_dir = repo_root / "data" / "cleaned"
    cleaned_dir.mkdir(parents=True, exist_ok=True)

    datasets: dict[str, Path] = {
        "customers": repo_root / "data" / "raw" / "customers_data.csv",
        "products": repo_root / "data" / "raw" / "products_data.csv",
        "sales": repo_root / "data" / "raw" / "sales_data.csv",
    }

    # Import DataScrubber after src is on sys.path
    try:
        from analytics_project.data_preparation.data_scrubber import DataScrubber  # type: ignore
    except Exception as exc:
        logger.exception("Failed to import DataScrubber: %s", exc)
        raise

    for name, path in datasets.items():
        logger.info("Processing %s dataset: %s", name, path)
        if not path.exists():
            logger.warning("File not found, skipping: %s", path)
            continue

        try:
            df = pd.read_csv(path)
        except Exception as exc:
            logger.exception("Failed to read %s: %s", path, exc)
            continue

        try:
            scrubber = DataScrubber(df)

            # Apply cleaning steps (DataScrubber methods are expected to exist)
            scrubber.standardize_column_names()
            scrubber.handle_missing_data(fill_value="Unknown")
            scrubber.remove_duplicate_records()

            logger.info("%s dataset: inspection after cleaning", name.capitalize())
            scrubber.inspect()

            output_path = cleaned_dir / f"{name}_cleaned.csv"
            scrubber.save(output_path)
            logger.info("Saved cleaned dataset to %s", output_path)
        except Exception as exc:
            logger.exception("Failed processing %s dataset: %s", name, exc)
            continue


def main() -> int:
    """Entry point for the demo scrubber script.

    Returns
    -------
    int
        Exit code (0 on success, non-zero on failure).
    """
    repo_root = configure_path()
    logger = get_logger()

    logger.info("Starting demo_scrubber.py")
    try:
        process_datasets(repo_root, logger)
    except Exception:
        logger.exception("Demo scrubber terminated with errors")
        return 1
    logger.info("Demo scrubber finished successfully")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
