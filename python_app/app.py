import pandas as pd
import numpy as np
from pathlib import Path
from sqlalchemy import create_engine, URL
# ---------- Utility Functions ----------

def format_book_title(title):
    return title if title.isupper() else title.title()

def writeMetricsToSQL(dropCount, customer_drop_count, engine):
    metrics = {
        'Record Loss: Loans': dropCount,
        'Record Loss: Customers': customer_drop_count
    }
    metrics_df = pd.DataFrame([metrics])
    metrics_df.to_sql('DE_metrics', con=engine, if_exists='replace', index=False)
    print("DE metrics written to SQL.")

# ---------- Modular Pipeline Steps ----------

def fileLoader(path):
    df1 = pd.read_csv(path / "03_Library Systembook.csv")
    df2 = pd.read_csv(path / "03_Library SystemCustomers.csv")
    return df1, df2

def duplicateCheck(df1, df2):
    df1 = df1.drop_duplicates()
    df2 = df2.drop_duplicates()
    return df1, df2

def naCheck(df1, df2):
    # Count before
    loan_records_before = len(df1)
    customer_records_before = len(df2)

    df1.columns = df1.columns.str.strip().str.lower().str.replace(" ", "_")
    df2.columns = df2.columns.str.strip().str.lower().str.replace(" ", "_")

    df1 = df1.dropna(how='all')
    df2 = df2.dropna(how='all')

    df1 = df1.dropna(subset=["customer_id"])

    df1 = df1.fillna("Unknown")
    df2 = df2.fillna("Unknown")

    df1 = df1.applymap(lambda x: x.strip() if isinstance(x, str) else x)
    df2 = df2.applymap(lambda x: x.strip() if isinstance(x, str) else x)
    
    # Count after and calculate drops
    loan_records_after = len(df1)
    customer_records_after = len(df2)
    dropCount = loan_records_before - loan_records_after
    customer_drop_count = customer_records_before - customer_records_after

    return df1, df2, dropCount, customer_drop_count

def dateCleaner(df1, dropCount):
    df1['book_checkout'] = (
        df1['book_checkout']
        .astype(str)
        .str.replace('"', '', regex=False)
        .str.strip()
        .replace('nan', pd.NA)
    )

    df1['book_checkout'] = pd.to_datetime(df1['book_checkout'], format='mixed', errors='coerce')
    df1['book_returned'] = pd.to_datetime(df1['book_returned'], format='mixed', errors='coerce')

    df1 = df1.dropna(subset=['book_checkout', 'book_returned'])

    dropCount +=1

    return df1

def dataEnrich(na_dropped_data, dropCount):
    """
    Enrich the data by computing the loan duration in days
    and removing invalid records with negative duration.
    """
    data_enriched = na_dropped_data.copy()

    data_enriched['loan_duration'] = (
        data_enriched['book_returned'] - data_enriched['book_checkout']
    ).dt.days

    valid_loan_data = data_enriched[data_enriched['loan_duration'] >= 0]

    dropCount += len(data_enriched) - len(valid_loan_data)

    return valid_loan_data, dropCount

def saveCleanedFiles(df1, df2, path):
    cleaned_path = path / "cleaned"
    cleaned_path.mkdir(exist_ok=True)
    df1.to_csv(cleaned_path / "book_cleaned.csv", index=False)
    df2.to_csv(cleaned_path / "customers_cleaned.csv", index=False)
    print("Cleaning complete. Files saved in 'data/cleaned/'.")

def addToSQL(df1, df2, engine):

    df1.to_sql("books", con=engine, if_exists='replace', index=False)
    df2.to_sql("customers", con=engine, if_exists='replace', index=False)
    print("Data loaded to SQL Server successfully.")

# ---------- Main Execution ----------

if __name__ == "__main__":
    data_path = Path("C:/Users/Admin/Desktop/DE5_M5_20250728/python_app/data")

    # Step-by-step execution
    df1, df2 = fileLoader(data_path)
    df1, df2 = duplicateCheck(df1, df2)
    df1, df2, dropCount, customer_drop_count = naCheck(df1, df2)
    df1 = dateCleaner(df1, dropCount)
    df1, dropCount = dataEnrich(df1, dropCount)
    saveCleanedFiles(df1, df2, data_path)

    # SQL upload
    connection_url = URL.create(
        "mssql+pyodbc",
        query={
            "driver": "ODBC Driver 17 for SQL Server",
            "trusted_connection": "yes"
        },
        host="localhost",
        database="DE5_Module5"
    )
    engine = create_engine(connection_url)
    
    addToSQL(df1, df2, engine)
    writeMetricsToSQL(dropCount, customer_drop_count, engine)
