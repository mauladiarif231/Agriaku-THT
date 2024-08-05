from pipeline.utils.file_utils import write_parquet_file
from pipeline.utils.logger import setup_logging

import pandas as pd
import os

logger = setup_logging('pipeline_log.log')

def ingest_raw_csv_table(csv_path: str, parquet_path: str):
    logger.info(f"Ingesting data from {os.path.basename(csv_path)} to Parquet.")
    df = pd.read_csv(csv_path)
    write_parquet_file(df, parquet_path)
    logger.info(f"Data ingested to {parquet_path}")

if __name__ == '__main__':
    ingest_raw_csv_table('data/raw/course_attendance.csv', 'data/datamart/course_attendance.parquet')