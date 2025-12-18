import os
import pandas as pd
import json
import pickle
from pathlib import Path

def load_file(file_path):
    ext = Path(file_path).suffix.lower()
    if ext == '.csv':
        return pd.read_csv(file_path)
    elif ext == '.xlsx':
        return pd.read_excel(file_path)
    elif ext == '.json':
        with open(file_path, 'r') as f:
            data = json.load(f)
        if isinstance(data, list):
            return pd.DataFrame(data)
        elif isinstance(data, dict):
            # Assume it's a dict of records
            return pd.DataFrame(list(data.values()))
        else:
            return pd.DataFrame([data])
    elif ext == '.pickle':
        with open(file_path, 'rb') as f:
            data = pickle.load(f)
        if isinstance(data, list):
            return pd.DataFrame(data)
        elif hasattr(data, 'shape'):  # numpy array
            if data.ndim == 3 and data.shape[0] == 1:
                data = data.squeeze(0)  # remove first dim if 1
            return pd.DataFrame(data)
        else:
            return pd.DataFrame([data])
    elif ext == '.parquet':
        return pd.read_parquet(file_path)
    elif ext == '.html':
        tables = pd.read_html(file_path)
        if tables:
            return tables[0]  # Assuming first table
        else:
            return pd.DataFrame()
    else:
        print(f"Unsupported file type: {ext}")
        return pd.DataFrame()

def profile_dataset(file_path, df):
    print(f"\n--- Profiling {file_path} ---")
    print(f"Shape: {df.shape}")
    print("Columns and Data Types:")
    print(df.dtypes)
    print("\nFirst 5 rows:")
    print(df.head())
    print("\nNull counts:")
    print(df.isnull().sum())
    print("\nDuplicate rows:", df.duplicated().sum())
    # Basic quality checks
    quality_issues = []
    for col in df.columns:
        if df[col].isnull().sum() > 0:
            quality_issues.append(f"Nulls in {col}")
        if df[col].duplicated().sum() > len(df) * 0.1:  # Arbitrary threshold
            quality_issues.append(f"Potential duplicates in {col}")
    if quality_issues:
        print("Quality Issues:", quality_issues)
    else:
        print("No major quality issues detected.")

def main():
    dataset_dir = Path("Project Dataset")
    for file_path in dataset_dir.rglob("*"):
        if file_path.is_file():
            df = load_file(file_path)
            if not df.empty:
                profile_dataset(file_path, df)

if __name__ == "__main__":
    main()

