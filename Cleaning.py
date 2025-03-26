import pandas as pd

# Load data
file_path = 'path_to_your_file.csv'  # Update with your actual file path
df = pd.read_csv(r"C:\Users\sande\.cache\kagglehub\datasets\iveeaten3223times\massive-yahoo-finance-dataset\versions\2\stock_details_5_years.csv")

# Display column names
print("Original Columns:")
print(df.columns)

# Cleaning column names
df.columns = df.columns.str.strip().str.lower().str.replace(' ', '_').str.replace('[^a-zA-Z0-9_]', '')
print("\nCleaned Columns:")
print(df.columns)

# Handling missing values
print("\nMissing Values Before Cleaning:")
print(df.isnull().sum())
df = df.dropna()  # Remove rows with missing values

# Convert date column if present
if 'date' in df.columns:
    df['date'] = pd.to_datetime(df['date'], errors='coerce')
    df = df.dropna(subset=['date'])

# Removing duplicates
df = df.drop_duplicates()

# Handling outliers using IQR method
numeric_cols = df.select_dtypes(include=['float64', 'int64']).columns
for col in numeric_cols:
    Q1 = df[col].quantile(0.25)
    Q3 = df[col].quantile(0.75)
    IQR = Q3 - Q1
    df = df[(df[col] >= (Q1 - 1.5 * IQR)) & (df[col] <= (Q3 + 1.5 * IQR))]

print("\nData Cleaning Complete. Final Data Shape:", df.shape)

# Save cleaned data
df.to_csv('cleaned_data.csv', index=False)
print("Cleaned data saved to cleaned_data.csv")
