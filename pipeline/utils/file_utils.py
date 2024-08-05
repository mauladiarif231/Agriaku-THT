# pipeline/utils/file_utils.py
import pandas as pd
import os

def write_parquet_file(df, path):
    """Write DataFrame to a Parquet file."""
    os.makedirs(os.path.dirname(path), exist_ok=True)
    df.to_parquet(path, index=False)

def read_parquet_file(path):
    """Read a Parquet file and return a DataFrame."""
    return pd.read_parquet(path)
