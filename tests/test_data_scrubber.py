import sys
import os
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "src")))


import unittest
import pandas as pd
from analytics_project.data_preparation.data_scrubber import DataScrubber

class TestDataScrubber(unittest.TestCase):

    def setUp(self):
        # Sample data for testing
        self.df = pd.DataFrame({
            "Name": [" Alice ", "Bob", "Alice ", None],
            "Age": [25, 30, 25, None],
            "Score": [90, 85, 90, 100]
        })
        self.scrubber = DataScrubber(self.df.copy())

    def test_standardize_column_names(self):
        self.scrubber.standardize_column_names()
        self.assertIn("name", self.scrubber.df.columns)
        self.assertIn("age", self.scrubber.df.columns)

    def test_remove_duplicate_records(self):
        self.scrubber.format_column_strings("Name", case="lower")
        self.scrubber.remove_duplicate_records()
        self.assertEqual(len(self.scrubber.df), 3)

    def test_handle_missing_data_fill(self):
        self.scrubber.handle_missing_data(fill_value="Missing")
        self.assertFalse(self.scrubber.df.isnull().any().any())

    def test_handle_missing_data_drop(self):
        scrubber = DataScrubber(self.df.copy())
        scrubber.handle_missing_data(drop=True)
        self.assertEqual(len(scrubber.df), 3)

    def test_format_column_strings_lower(self):
        self.scrubber.format_column_strings("Name", case="lower")
        self.assertEqual(self.scrubber.df["Name"].iloc[0], "alice")

    def test_convert_column_type(self):
        self.scrubber.convert_column_type("Age", float)
        self.assertTrue(pd.api.types.is_float_dtype(self.scrubber.df["Age"]))

    def test_filter_outliers(self):
        self.scrubber.filter_outliers("Score", min_val=80, max_val=95)
        self.assertTrue((self.scrubber.df["Score"] <= 95).all())
        self.assertTrue((self.scrubber.df["Score"] >= 80).all())

    def test_parse_date_column(self):
        df = pd.DataFrame({"date": ["2023-01-01", "2023-02-01"]})
        scrubber = DataScrubber(df)
        scrubber.parse_date_column("date", new_column="parsed")
        self.assertTrue(pd.api.types.is_datetime64_any_dtype(scrubber.df["parsed"]))

    def test_rename_columns(self):
        self.scrubber.rename_columns({"Name": "FullName"})
        self.assertIn("FullName", self.scrubber.df.columns)

    def test_reorder_columns(self):
        self.scrubber.reorder_columns(["Score", "Age", "Name"])
        self.assertEqual(list(self.scrubber.df.columns), ["Score", "Age", "Name"])

if __name__ == "__main__":
    unittest.main()
