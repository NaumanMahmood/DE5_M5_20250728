import pandas as pd
from pathlib import Path

# Define the path to your data folder
data_path = Path("C:/Users/Admin/Desktop/DE5_M5_20250728/python_app/data")

df1 = pd.read_csv(data_path / "03_Library Systembook.csv")
df2 = pd.read_csv(data_path / "03_Library SystemCustomers.csv")


# Step 2: Clean the data

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

# Step 3: Save the cleaned files (to a cleaned/ folder, for example)
cleaned_path = data_path / "cleaned"
cleaned_path.mkdir(exist_ok=True)

df1.to_csv(cleaned_path / "book_cleaned.csv", index=False)
df2.to_csv(cleaned_path / "customers_cleaned.csv", index=False)

print("Cleaning complete. Files saved in 'data/cleaned/'.")