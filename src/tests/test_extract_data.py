# test_extract_data.py

import unittest
from extract_data import extract
import pandas as pd

class TestExtractData(unittest.TestCase):

    def test_extract(self):
        data_filepath = '../data/test_data.json'
        result = extract(data_filepath)

        # Assert that the result is a DataFrame
        self.assertIsInstance(result, pd.DataFrame)

        # Add more specific tests based on the expected structure of the DataFrame
        self.assertGreater(len(result), 0)
        self.assertTrue('transactionId' in result.columns)
        self.assertTrue('currency' in result.columns)
        self.assertTrue(len(result.columns), 9)
        # Add more specific tests based on the expected behavior of extract
