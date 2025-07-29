import pandas as pd
import numpy as np
from pathlib import Path
from sqlalchemy import create_engine, URL

# ---------- Utility Functions ----------

def format_book_title(title):
    return title if title.isupper() else title.title()

def period_to_days(period_str):
    if pd.isna(period_str):
        return np.nan
    period_str = period_str.lower().strip()
    number = int(''.join(filter(str.isdigit, period_str)))  # extract number

    if "week" in period_str:
        return number * 7
    elif "month" in period_str:
        return number * 30
    elif "day" in period_str:
        return number
    else:
        return np.nan  # unknown format

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
    df1.columns = df1.columns.str.strip().str.lower().str.replace(" ", "_")
    df2.columns = df2.columns.str.strip().str.lower().str.replace(" ", "_")

    df1 = df1.dropna(how='all')
    df2 = df2.dropna(how='all')

    df1 = df1.dropna(subset=["customer_id"])

    df1 = df1.fillna("Unknown")
    df2 = df2.fillna("Unknown")

    df1 = df1.applymap(lambda x: x.strip() if isinstance(x, str) else x)
    df2 = df2.applymap(lambda x: x.strip() if isinstance(x, str) else x)
    
    return df1, df2

def dateCleaner(df1):
    df1['book_checkout'] = (
        df1['book_checkout']
        .astype(str)
        .str.replace('"', '', regex=False)
        .str.strip()
        .replace('nan', pd.NA)
    )

    df1['book_checkout'] = pd.to_datetime(df1['book_checkout'], errors='coerce')
    df1['book_returned'] = pd.to_datetime(df1['book_returned'], errors='coerce')

    df1['days_int'] = df1['days_allowed_to_borrow'].apply(period_to_days)

    cutoff_date = pd.Timestamp.today() + pd.DateOffset(years=1)

    df1['book_checkout'] = df1.apply(
        lambda row: (row['book_returned'] - pd.Timedelta(days=row['days_int']))
        if (pd.isna(row['book_checkout']) or row['book_checkout'] > cutoff_date)
        else row['book_checkout'],
        axis=1
    )

    return df1

def dataEnrich(df1, df2):
    df1['id'] = pd.to_numeric(df1['id'], errors='coerce').astype('Int64')
    df1['customer_id'] = pd.to_numeric(df1['customer_id'], errors='coerce').astype('Int64')
    df2['customer_id'] = pd.to_numeric(df2['customer_id'], errors='coerce').astype('Int64')

    df1["books"] = df1["books"].apply(format_book_title)

    df1.drop_duplicates(subset=['books', 'book_checkout', 'customer_id'], keep='first', inplace=True)

    mask = df1["book_checkout"] > df1["book_returned"]
    df1.loc[mask, ["book_checkout", "book_returned"]] = df1.loc[mask, ["book_returned", "book_checkout"]].values

    # Add derived column: days between checkout and return
    df1["borrowed_days"] = (df1["book_returned"] - df1["book_checkout"]).dt.days

    df1.drop(columns=["days_int"], inplace=True)

    return df1, df2

def saveCleanedFiles(df1, df2, path):
    cleaned_path = path / "cleaned"
    cleaned_path.mkdir(exist_ok=True)
    df1.to_csv(cleaned_path / "book_cleaned.csv", index=False)
    df2.to_csv(cleaned_path / "customers_cleaned.csv", index=False)
    print("✅ Cleaning complete. Files saved in 'data/cleaned/'.")

def addToSQL(df1, df2, database="Library"):
    connection_url = URL.create(
        "mssql+pyodbc",
        query={
            "driver": "ODBC Driver 17 for SQL Server",
            "trusted_connection": "yes"
        },
        host="localhost",
        database=database
    )
    engine = create_engine(connection_url)
    df1.to_sql("books", con=engine, if_exists='replace', index=False)
    df2.to_sql("customers", con=engine, if_exists='replace', index=False)
    print("✅ Data loaded to SQL Server successfully.")

# ---------- Main Execution ----------

if __name__ == "__main__":
    data_path = Path("C:/Users/Admin/Desktop/DE5_M5_20250728/python_app/data")

    # Step-by-step execution
    df1, df2 = fileLoader(data_path)
    df1, df2 = duplicateCheck(df1, df2)
    df1, df2 = naCheck(df1, df2)
    df1 = dateCleaner(df1)
    df1, df2 = dataEnrich(df1, df2)
    saveCleanedFiles(df1, df2, data_path)
    addToSQL(df1, df2)