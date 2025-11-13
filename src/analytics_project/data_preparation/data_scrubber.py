"""Data scrubber utilities.

This module provides the :class:`DataScrubber` helper for common
data-cleaning operations on pandas DataFrames. It is intended to keep
small, well-documented, and testable transformations in one place so
other scripts can reuse them.

Public API
----------
DataScrubber
    Lightweight, chainable operations for preparing tabular datasets.
"""

import io
import pandas as pd
from pathlib import Path
import re

from analytics_project.utils.logger import logger

__all__ = ["DataScrubber"]


def camel_to_snake(name: str) -> str:
    name = name.strip()
    # Insert underscore between lowercase-uppercase boundaries
    name = re.sub(r"(?<=[a-z0-9])(?=[A-Z])", "_", name)
    return name.lower()


class DataScrubber:
    """A small pipeline of reusable DataFrame cleaning helpers.

    Contract (inputs/outputs):
    - Input: a pandas.DataFrame provided at construction
    - Output: methods mutate ``self.df`` in-place and return it for
      chaining. Methods raise :class:`ValueError` for missing columns.
    """

    def __init__(self, df: pd.DataFrame):
        """Initialize with a DataFrame to be cleaned.

        Parameters
        ----------
        df : pd.DataFrame
            The DataFrame to operate on. The object stores this as
            ``self.df`` and updates it in-place.
        """
        self.df = df

    def standardize_column_names(self) -> pd.DataFrame:
        """Convert camelCase and spaced column names to snake_case."""
        self.df.columns = [camel_to_snake(col) for col in self.df.columns]
        return self.df

    def standardize_categorical_column(self, column: str) -> pd.DataFrame:
        """Standardize categorical values by stripping whitespace and title-casing."""
        self.df[column] = (
            self.df[column]
            .astype(str)
            .str.strip()
            .str.title()  # Converts 'east' → 'East', 'SOUTH-WEST' → 'South-West'
        )
        return self.df

    def remove_duplicate_records(self, subset=None) -> pd.DataFrame:
        """Drop duplicate rows from the DataFrame. Optionally specify a subset of columns to check."""
        if subset:
            dupes = self.df[self.df.duplicated(subset=subset, keep=False)]
            if not dupes.empty:
                print(f"Duplicate rows based on {subset}:\n", dupes)
            self.df = self.df.drop_duplicates(subset=subset, keep="first")
        else:
            self.df = self.df.drop_duplicates()
        return self.df

    def save(self, path: str | Path) -> None:
        """Save the cleaned DataFrame to a CSV file.

        Parameters
        ----------
        path : str or Path
            Destination file path for the CSV. Parent directories will be
            created if necessary.
        """
        path = Path(path)
        path.parent.mkdir(parents=True, exist_ok=True)
        try:
            self.df.to_csv(path, index=False)
        except Exception as exc:  # pragma: no cover - IO interaction
            raise OSError(f"Failed to save DataFrame to {path}: {exc}") from exc

    def handle_missing_data(
        self, drop: bool = False, fill_value: str | int | float | None = None
    ) -> pd.DataFrame:
        """Handle missing values by dropping or filling.

        Parameters
        ----------
        drop : bool, optional
            If True, drop rows with missing values (default False).
        fill_value : str | int | float | None, optional
            Value to use when filling missing entries. If ``None`` and
            ``drop`` is False, the DataFrame is left unchanged for missing
            values.

        Returns
        -------
        pd.DataFrame
            The cleaned DataFrame (same object as ``self.df``).
        """
        if drop:
            self.df = self.df.dropna()
        elif fill_value is not None:
            self.df = self.df.fillna(fill_value)
        return self.df

    def format_column_strings(self, column: str, case: str = "lower") -> pd.DataFrame:
        """Format strings in a column: trim whitespace and apply casing.

        Parameters
        ----------
        column : str
            Column name to format.
        case : str, optional
            One of ``"lower"`` or ``"upper"`` specifying the casing to
            apply. Default is ``"lower"``.

        Returns
        -------
        pd.DataFrame
            The DataFrame with the formatted column.
        """
        if column not in self.df.columns:
            raise ValueError(f"Column '{column}' not found.")
        if case == "lower":
            self.df[column] = self.df[column].astype(str).str.lower().str.strip()
        elif case == "upper":
            self.df[column] = self.df[column].astype(str).str.upper().str.strip()
        return self.df

    def rename_columns(self, mapping: dict[str, str]) -> pd.DataFrame:
        """Rename columns using a provided dictionary.

        Parameters
        ----------
        mapping : dict[str, str]
            Mapping of old column names to new column names.

        Returns
        -------
        pd.DataFrame
            The DataFrame with renamed columns.
        """
        for old in mapping:
            if old not in self.df.columns:
                raise ValueError(f"Column '{old}' not found.")
        self.df = self.df.rename(columns=mapping)
        return self.df

    def reorder_columns(self, order: list[str]) -> pd.DataFrame:
        """Reorder columns based on a specified list.

        Parameters
        ----------
        order : list[str]
            Desired column order. All names must exist in the DataFrame.

        Returns
        -------
        pd.DataFrame
            The DataFrame with reordered columns.
        """
        for col in order:
            if col not in self.df.columns:
                raise ValueError(f"Column '{col}' not found.")
        self.df = self.df[order]
        return self.df

    def filter_outliers(self, column: str, min_val: float, max_val: float) -> pd.DataFrame:
        """Remove rows where column values fall outside bounds.

        Parameters
        ----------
        column : str
            Column to filter.
        min_val : float
            Minimum allowed value (inclusive).
        max_val : float
            Maximum allowed value (inclusive).

        Returns
        -------
        pd.DataFrame
            The DataFrame with out-of-range rows removed.
        """
        if column not in self.df.columns:
            raise ValueError(f"Column '{column}' not found.")
        self.df[column] = pd.to_numeric(self.df[column], errors="coerce")
        before = len(self.df)
        self.df = self.df[(self.df[column] >= min_val) & (self.df[column] <= max_val)]
        after = len(self.df)
        print(
            f"[filter_outliers] Removed {before - after} rows from '{column}' outside [{min_val}, {max_val}]"
        )
        return self.df

    def convert_column_type(self, column: str, dtype: type) -> pd.DataFrame:
        """Convert a column to a specified data type.

        Parameters
        ----------
        column : str
            Column to convert.
        dtype : type
            Target data type (e.g. ``int``, ``float``, ``str``).

        Returns
        -------
        pd.DataFrame
            The DataFrame with the converted column.
        """
        if column not in self.df.columns:
            raise ValueError(f"Column '{column}' not found.")
        self.df[column] = self.df[column].astype(dtype)
        return self.df

    def parse_date_column(self, column: str, new_column: str = "parsed_date") -> pd.DataFrame:
        """Parse a column as datetime and store in a new column.

        Parameters
        ----------
        column : str
            Column to parse as dates.

        new_column : str, optional
            Name for the new datetime column (default ``"parsed_date"``).

        Returns
        -------
        pd.DataFrame
            The DataFrame with the parsed date column appended.
        """
        if column not in self.df.columns:
            raise ValueError(f"Column '{column}' not found.")
        self.df[new_column] = pd.to_datetime(self.df[column], errors="coerce")
        return self.df

    def inspect(self) -> tuple[str, str]:
        """Return info and summary statistics as strings."""
        buffer = io.StringIO()
        self.df.info(buf=buffer)
        info = buffer.getvalue()
        summary = self.df.describe(include="all").to_string()
        return info, summary

    def remove_negative_values(self, column: str) -> pd.DataFrame:
        """Remove rows where the specified column has negative values.

        Parameters
        ----------
        column : str
            Column to check for negative values.

        Returns
        -------
        pd.DataFrame
            The DataFrame with negative values removed.
        """
        if column not in self.df.columns:
            raise ValueError(f"Column '{column}' not found.")
        self.df = self.df[self.df[column] >= 0]
        return self.df

    def convert_empty_strings_to_na(self) -> pd.DataFrame:
        """Convert empty strings in the DataFrame to missing values (NaN).

        Returns
        -------
        pd.DataFrame
            The DataFrame with empty strings replaced by NaN.
        """
        self.df = self.df.replace("", pd.NA)
        return self.df

    def override_invalid_dates(
        self, column: str = "SaleDate", fixed_date: str = "2025-05-04"
    ) -> pd.DataFrame:
        """Replace invalid or missing dates with a fixed value."""
        parsed_dates = pd.to_datetime(self.df[column], errors="coerce")
        invalid_mask = parsed_dates.isna()

        if invalid_mask.any():
            print(
                f"[Scrubber] Overriding {invalid_mask.sum()} invalid '{column}' values with '{fixed_date}'."
            )
            self.df.loc[invalid_mask, column] = fixed_date

        return self.df

    def correct_zero_sales_discount(self) -> pd.DataFrame:
        """Update discount_percent to 100 where sale_amount is 0.00 and discount is missing or invalid."""

        # Step 1: Normalize types
        self.df["sale_amount"] = pd.to_numeric(self.df["sale_amount"], errors="coerce")
        self.df["discount_percent"] = pd.to_numeric(self.df["discount_percent"], errors="coerce")

        # Step 2: Define correction condition
        condition = (self.df["sale_amount"] == 0.0) & (
            (self.df["discount_percent"].isna()) | (self.df["discount_percent"] < 100.0)
        )

        # Step 3: Apply correction
        updated_count = condition.sum()
        self.df.loc[condition, "discount_percent"] = 100.0

        # Step 4: Log result
        try:
            from analytics_project.utils.logger import logger

            logger.info(
                f"Updated {updated_count} rows: sale_amount = 0.00 → discount_percent set to 100.0"
            )
        except ImportError:
            print(
                f"Updated {updated_count} rows: sale_amount = 0.00 → discount_percent set to 100.0"
            )

        return self.df
