import unittest
from data_quality_check import quality_check
import pandas as pd

class TestDQC(unittest.TestCase):

    def test_quality_check_valid_transactions(self):
        # Create a sample DataFrame with valid transactions
        df = pd.DataFrame({
            'transactionId': ['1', '2', '3'],
            'customerId': ['A', 'B', 'C'],
            'transactionDate': ['2021-01-01', '2021-01-02', '2021-01-03'],
            'currency': ['EUR', 'USD', 'GBP'],
            'amount': [100, 200, 300],
            'description': ['Desc1', 'Desc2', 'Desc3']
        })

        result = quality_check(df)

        # Validate that all transactions pass the quality checks
        self.assertEqual(len(result), 3)
        self.assertTrue(all(result['currency'].isin(["EUR", "USD", "GBP"])))

    def test_quality_check_invalid_currency(self):
        # Create a sample DataFrame with invalid currency
        df = pd.DataFrame({
            'transactionId': ['1', '2', '3'],
            'customerId': ['A', 'B', 'C'],
            'transactionDate': ['2021-01-01', '2021-01-02', '2021-01-03'],
            'currency': ['EUR', 'USD', 'INVALID'],
            'amount': [100, 200, 300],
            'description': ['Desc1', 'Desc2', 'Desc3']
        })

        result = quality_check(df)

        # Validate that transactions with invalid currency are filtered out
        self.assertEqual(len(result), 2)
        self.assertFalse(any(result['currency'] == 'INVALID'))

    def test_quality_check_invalid_transaction_date(self):
        # Create a sample DataFrame with invalid transactionDate
        df = pd.DataFrame({
            'transactionId': ['1', '2', '3'],
            'customerId': ['A', 'B', 'C'],
            'transactionDate': ['2021-01-01', '2022-93-93', '2021-01-03'],
            'currency': ['EUR', 'USD', 'GBP'],
            'amount': [100, 200, 300],
            'description': ['Desc1', 'Desc2', 'Desc3']
        })

        result = quality_check(df)

        # Validate that transactions with invalid transactionDate are filtered out
        self.assertEqual(len(result), 2)
        self.assertFalse(any(result['transactionDate'] == '2022-93-93'))

