import pandas as pd
import numpy as np
from pathlib import Path


def period_to_days(period_str):
    if pd.isna(period_str):
        return np.nan
    period_str = period_str.lower().strip()
    
    number = int(''.join(filter(str.isdigit, period_str)))  # extract number
    
    if "week" or "weeks" in period_str:
        return number * 7
    elif "month" or "months" in period_str:
        return number * 30  # Approximate month as 30 days
    elif "day" or "days" in period_str:
        return number
    else:
        return np.nan  # unknown format

# Define the path to your data folder
data_path = Path("C:/Users/Admin/Desktop/DE5_M5_20250728/python_app/data")

df1 = pd.read_csv(data_path / "03_Library Systembook.csv")
df2 = pd.read_csv(data_path / "03_Library SystemCustomers.csv")


# Step 2: Clean the data

# Clean column names for both DataFrames
df1.columns = df1.columns.str.strip().str.lower().str.replace(" ", "_")
df2.columns = df2.columns.str.strip().str.lower().str.replace(" ", "_")

# Drop duplicates
df1 = df1.drop_duplicates()
df2 = df2.drop_duplicates()

# Drop rows with all missing values
df1 = df1.dropna(how='all')
df2 = df2.dropna(how='all')

# Fill missing values with placeholder
df1 = df1.fillna("Unknown")
df2 = df2.fillna("Unknown")

# Strip whitespace from string columns
df1 = df1.applymap(lambda x: x.strip() if isinstance(x, str) else x)
df2 = df2.applymap(lambda x: x.strip() if isinstance(x, str) else x)

# Step 2.1: extra cleaning

# Remove quotes and convert to datetime
df1['book_checkout'] = (
    df1['book_checkout']
    .astype(str)                       # Ensure all values are strings
    .str.replace('"', '', regex=False)  # Remove double quotes
    .str.strip()                       # Remove whitespace if any
    .replace('nan', pd.NA)             # Handle actual string "nan"
)

# Convert to datetime
df1['book_checkout'] = pd.to_datetime(df1['book_checkout'], errors='coerce')
df1['book_returned'] = pd.to_datetime(df1['book_returned'], errors='coerce')

# Created a new column with integer day
df1['days_int'] = df1['days_allowed_to_borrow'].apply(period_to_days)

# Define cutoff date - for example, 1 year from today
cutoff_date = pd.Timestamp.today() + pd.DateOffset(years=1)

# Fill missing or "too far in future" book_checkout dates by calculation
df1['book_checkout'] = df1.apply(
    lambda row: (row['book_returned'] - pd.Timedelta(days=row['days_int']))
    if (pd.isna(row['book_checkout']) or row['book_checkout'] > cutoff_date)
    else row['book_checkout'],
    axis=1
)

# Example: convert ID columns to integer
df1['id'] = pd.to_numeric(df1['id'], errors='coerce').astype('Int64')
df1['customer_id'] = pd.to_numeric(df1['customer_id'], errors='coerce').astype('Int64')
df2['customer_id'] = pd.to_numeric(df2['customer_id'], errors='coerce').astype('Int64')

# Step 3: Save the cleaned files (to a cleaned/ folder, for example)
cleaned_path = data_path / "cleaned"
cleaned_path.mkdir(exist_ok=True)

df1.to_csv(cleaned_path / "book_cleaned.csv", index=False)
df2.to_csv(cleaned_path / "customers_cleaned.csv", index=False)

print("Cleaning complete. Files saved in 'data/cleaned/'.")