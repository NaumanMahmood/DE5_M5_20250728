import unittest
import pandas as pd
import sys
import os

# Add project root to the path
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from python_app.app import bookstore_app  # Correct import for app.py inside python_app

class TestBookstoreApp(unittest.TestCase):

    def setUp(self):
        self.app = bookstore_app.__new__(bookstore_app)  # Avoid running __init__
        self.app.metrics = {
            'invalid_loan_duration_count': 0
        }
        self.app.df1 = None
        self.app.df2 = None

    def test_dataEnrich_remove_negative_durations(self):
        self.app.df1 = pd.DataFrame({
            'book_checkout': pd.to_datetime(['2023-01-05', '2023-01-10']),
            'book_returned': pd.to_datetime(['2023-01-04', '2023-01-15'])
        })

        self.app.dataEnrich()

        self.assertEqual(len(self.app.df1), 1)
        self.assertEqual(self.app.df1['loan_duration'].iloc[0], 5)
        self.assertEqual(self.app.metrics['invalid_loan_duration_count'], 1)

    def test_naCheck_drops_missing_customer_id(self):
        self.app.df1 = pd.DataFrame({
            'customer_id': [123, None, 789, None],
            'book_checkout': ['2023-01-05', '2023-02-10', None, None],
            'book_returned': [None, '2023-03-25', None, None],
            'books': ['A', 'B', 'C', None]
        })
        self.app.df2 = pd.DataFrame({
            'customer_name': ['Alice', None, None],
            'customer_id': [1, 2, None]
        })

        self.app.naCheck()

        self.assertEqual(len(self.app.df1), 2) # 2 dropped
        self.assertEqual(len(self.app.df2), 2) # 1 dropped, 2 left
        self.assertEqual(self.app.metrics['na_in_book_checkout'], 2)
        self.assertEqual(self.app.metrics['na_in_book_returned'], 3)

if __name__ == '__main__':
    unittest.main()