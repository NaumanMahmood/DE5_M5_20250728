# python_app/tests/tests.py

import unittest
import pandas as pd
from app import bookstore_app

class TestBookstoreApp(unittest.TestCase):

    def setUp(self):
        # Dummy instance
        self.app = bookstore_app.__new__(bookstore_app) 
        self.app.dropCount = 0
        self.app.customer_drop_count = 0

    def test_dataEnrich_remove_negative_durations(self):
        self.app.df1 = pd.DataFrame({
            'book_checkout': pd.to_datetime(['2023-01-05', '2023-01-10']),
            'book_returned': pd.to_datetime(['2023-01-04', '2023-01-15']) 
        })

        self.app.dataEnrich()

        # Expect only one valid row left
        self.assertEqual(len(self.app.df1), 1)
        self.assertEqual(self.app.df1['loan_duration'].iloc[0], 5)
        self.assertEqual(self.app.dropCount, 1)

    def test_naCheck_drops_missing_customer_id(self):
        self.app.df1 = pd.DataFrame({
            'customer_id': [123, None, 456],
            'books': ['A', 'B', 'C']
        })
        self.app.df2 = pd.DataFrame({
            'customer_name': ['Alice', None, None],
            'customer_id': [1, 2, None]
        })

        self.app.naCheck()

        self.assertEqual(len(self.app.df1), 2)  # 1 dropped
        self.assertEqual(len(self.app.df2), 2)  # None dropped since dropna(how='all')
        self.assertEqual(self.app.dropCount, 1)
        self.assertEqual(self.app.customer_drop_count, 1) # 1 dropped

if __name__ == '__main__':
    unittest.main()