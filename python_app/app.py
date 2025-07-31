import pandas as pd
import numpy as np
import os
from sqlalchemy import create_engine, URL
import argparse

class bookstore_app:
    def __init__(self, mode='csv'):
        self.metrics = {
            'initial_loans': 0,
            'final_loans': 0,
            'initial_customers': 0,
            'final_customers': 0,
            'na_in_book_checkout': 0,
            'na_in_book_returned': 0,
            'invalid_loan_duration_count': 0,
            'loan_duplicates_removed': 0,
            'customer_duplicates_removed': 0,
        }
        self.df1 = None
        self.df2 = None
        self.mode = mode.lower()
        self.engine = None
        if self.mode == 'sql':
            self.engine = self._create_engine()

        self.run_pipeline()

    def _create_engine(self):
        connection_url = URL.create(
            "mssql+pyodbc",
            query={
                "driver": "ODBC Driver 17 for SQL Server",
                "trusted_connection": "yes"
            },
            host="localhost",
            database="DE5_Module5"
        )
        return create_engine(connection_url)

    def run_pipeline(self):
        self.fileLoader()
        self.duplicateCheck()
        self.naCheck()
        self.dateCleaner()
        self.dataEnrich()
        self.saveCleanedFiles()

        if self.mode == 'sql':
            self.addToSQL()
            self.writeMetricsToSQL()
        else:
            print("Skipped SQL upload as per mode selection.")

    def fileLoader(self):
        self.df1 = pd.read_csv('data/03_Library Systembook.csv')
        self.df2 = pd.read_csv('data/03_Library SystemCustomers.csv')

        self.metrics['initial_loans'] = len(self.df1)
        self.metrics['initial_customers'] = len(self.df2)

    def duplicateCheck(self):
        before_loans = len(self.df1)
        before_customers = len(self.df2)

        self.df1 = self.df1.drop_duplicates()
        self.df2 = self.df2.drop_duplicates()

        self.metrics['loan_duplicates_removed'] = before_loans - len(self.df1)
        self.metrics['customer_duplicates_removed'] = before_customers - len(self.df2)

    def naCheck(self):
        self.df1.columns = self.df1.columns.str.strip().str.lower().str.replace(" ", "_")
        self.df2.columns = self.df2.columns.str.strip().str.lower().str.replace(" ", "_")

        self.metrics['na_in_book_checkout'] = self.df1['book_checkout'].isna().sum()
        self.metrics['na_in_book_returned'] = self.df1['book_returned'].isna().sum()

        self.df1 = self.df1.dropna(how='all')
        self.df2 = self.df2.dropna(how='all')
        self.df1 = self.df1.dropna(subset=["customer_id"])

        self.df1 = self.df1.fillna("Unknown")
        self.df2 = self.df2.fillna("Unknown")

        self.df1 = self.df1.applymap(lambda x: x.strip() if isinstance(x, str) else x)
        self.df2 = self.df2.applymap(lambda x: x.strip() if isinstance(x, str) else x)

    def dateCleaner(self):
        self.df1['book_checkout'] = (
            self.df1['book_checkout']
            .astype(str)
            .str.replace('"', '', regex=False)
            .str.strip()
            .replace('nan', pd.NA)
        )

        self.df1['book_checkout'] = pd.to_datetime(self.df1['book_checkout'], format='mixed', errors='coerce')
        self.df1['book_returned'] = pd.to_datetime(self.df1['book_returned'], format='mixed', errors='coerce')

        self.df1 = self.df1.dropna(subset=['book_checkout', 'book_returned'])

    def dataEnrich(self):
        enriched = self.df1.copy()

        enriched['loan_duration'] = (
            enriched['book_returned'] - enriched['book_checkout']
        ).dt.days

        valid = enriched[enriched['loan_duration'] >= 0]
        
        invalid_rows = enriched[enriched['loan_duration'] < 0]
        self.metrics['invalid_loan_duration_count'] = len(invalid_rows)

        self.df1 = valid  # Replace with valid records

    def saveCleanedFiles(self):
        cleaned_path = "/app/output"
        os.makedirs(cleaned_path, exist_ok=True)

        self.metrics['final_loans'] = len(self.df1)
        self.metrics['final_customers'] = len(self.df2)

        self.df1.to_csv(os.path.join(cleaned_path, "book_cleaned.csv"), index=False)
        self.df2.to_csv(os.path.join(cleaned_path, "customers_cleaned.csv"), index=False)
        print("Cleaning complete. Files saved in 'data/cleaned/'.")

    def addToSQL(self):
        self.df1.to_sql("books", con=self.engine, if_exists='replace', index=False)
        self.df2.to_sql("customers", con=self.engine, if_exists='replace', index=False)
        print("Data loaded to SQL Server successfully.")

    def writeMetricsToSQL(self):
        metrics_df = pd.DataFrame([self.metrics])
        metrics_df.to_sql('DE_metrics', con=self.engine, if_exists='replace', index=False)
        print("Enhanced DE metrics written to SQL.")

# ---------- Run the pipeline ----------
if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Bookstore data cleaning and loading pipeline")
    parser.add_argument(
        "--mode",
        choices=["csv", "sql"],
        default="csv",
        help="Choose 'csv' to only save cleaned CSVs, or 'sql' to save CSVs and upload to SQL Server."
    )
    args = parser.parse_args()

    app = bookstore_app(mode=args.mode)
