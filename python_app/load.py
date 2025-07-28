import pandas as pd
from sqlalchemy import create_engine, URL
from pathlib import Path

data_path = Path("C:/Users/Admin/Desktop/DE5_M5_20250728/python_app/data")

# --- Connection Setup ---
connection_url = URL.create(
    "mssql+pyodbc",
    query={
        "driver": "ODBC Driver 17 for SQL Server",
        "trusted_connection": "yes"
    },
    host="localhost",
    database="Library"  # 🔁 Replace this with your DB name
)

engine = create_engine(connection_url)

# --- Load cleaned CSVs ---
df1 = pd.read_csv(data_path / "cleaned/book_cleaned.csv", parse_dates=['book_checkout', 'book_returned'])  # adjust date cols
df2 = pd.read_csv(data_path / "cleaned/customers_cleaned.csv")

# --- Load to SQL ---
df1.to_sql("books", con=engine, if_exists='replace', index=False)
df2.to_sql("customers", con=engine, if_exists='replace', index=False)

print("✅ Data loaded to SQL Server successfully.")
