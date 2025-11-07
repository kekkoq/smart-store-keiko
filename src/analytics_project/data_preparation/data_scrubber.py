"""
utils/data_scrubber.py

Reusable class for cleaning and preparing pandas DataFrames.
Designed for consistent operations across multiple datasets.

Includes methods for:
- Standardizing column names
- Removing duplicates
- Handling missing values
- Formatting string columns
- Filtering outliers
- Renaming and reordering columns
- Parsing date fields
"""

import pandas as pd
import io
from typing import List, Dict, Union, Tuple


class DataScrubber:
    def __init__(self, df: pd.DataFrame):
        """
        Initialize with a DataFrame to be cleaned.
        """
        self.df = df

    def standardize_column_names(self) -> pd.DataFrame:
        """
        Convert all column names to lowercase and replace spaces with underscores.
        """
        self.df.columns = [col.strip().lower().replace(" ", "_") for col in self.df.columns]
        return self.df

    def remove_duplicate_records(self) -> pd.DataFrame:
        """
        Drop duplicate rows from the DataFrame.
        """
        self.df = self.df.drop_duplicates()
        return self.df

    def save(self, path):
        """Save the cleaned DataFrame to a CSV file."""
        self.df.to_csv(path, index=False)

    from typing import Optional

    def handle_missing_data(
        self, drop: bool = False, fill_value: Optional[Union[str, int, float]] = None
    ) -> pd.DataFrame:
        """
        Handle missing values by dropping or filling.

        Parameters:
            drop (bool): If True, drop rows with missing values.
            fill_value: Value to fill missing entries.
        """
        if drop:
            self.df = self.df.dropna()
        elif fill_value is not None:
            self.df = self.df.fillna(fill_value)
        return self.df

    def format_column_strings(self, column: str, case: str = "lower") -> pd.DataFrame:
        """
        Format strings in a column: trim whitespace and apply casing.

        Parameters:
            column (str): Column to format.
            case (str): 'lower' or 'upper'
        """
        if column not in self.df.columns:
            raise ValueError(f"Column '{column}' not found.")
        if case == "lower":
            self.df[column] = self.df[column].astype(str).str.lower().str.strip()
        elif case == "upper":
            self.df[column] = self.df[column].astype(str).str.upper().str.strip()
        return self.df

    def rename_columns(self, mapping: Dict[str, str]) -> pd.DataFrame:
        """
        Rename columns using a provided dictionary.

        Parameters:
            mapping (dict): {old_name: new_name}
        """
        for old in mapping:
            if old not in self.df.columns:
                raise ValueError(f"Column '{old}' not found.")
        self.df = self.df.rename(columns=mapping)
        return self.df

    def reorder_columns(self, order: List[str]) -> pd.DataFrame:
        """
        Reorder columns based on a specified list.

        Parameters:
            order (list): Desired column order.
        """
        for col in order:
            if col not in self.df.columns:
                raise ValueError(f"Column '{col}' not found.")
        self.df = self.df[order]
        return self.df

    def filter_outliers(self, column: str, min_val: float, max_val: float) -> pd.DataFrame:
        """
        Remove rows where column values fall outside bounds.

        Parameters:
            column (str): Column to filter.
            min_val (float): Minimum allowed value.
            max_val (float): Maximum allowed value.
        """
        if column not in self.df.columns:
            raise ValueError(f"Column '{column}' not found.")
        self.df = self.df[(self.df[column] >= min_val) & (self.df[column] <= max_val)]
        return self.df

    def convert_column_type(self, column: str, dtype: type) -> pd.DataFrame:
        """
        Convert a column to a specified data type.

        Parameters:
            column (str): Column to convert.
            dtype (type): Target data type.
        """
        if column not in self.df.columns:
            raise ValueError(f"Column '{column}' not found.")
        self.df[column] = self.df[column].astype(dtype)
        return self.df

    def parse_date_column(self, column: str, new_column: str = "parsed_date") -> pd.DataFrame:
        """
        Parse a column as datetime and store in a new column.

        Parameters:
            column (str): Column to parse.
            new_column (str): Name of the new datetime column.
        """
        if column not in self.df.columns:
            raise ValueError(f"Column '{column}' not found.")
        self.df[new_column] = pd.to_datetime(self.df[column], errors="coerce")
        return self.df

    def inspect(self) -> Tuple[str, str]:
        """
        Return info and summary statistics as strings.
        """
        buffer = io.StringIO()
        self.df.info(buf=buffer)
        info = buffer.getvalue()
        summary = self.df.describe(include="all").to_string()
        return info, summary
