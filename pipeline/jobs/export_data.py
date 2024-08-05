from pipeline.utils.file_utils import read_parquet_file
from pipeline.utils.logger import setup_logging

import os

logger = setup_logging('pipeline_log.log')

def export_table_to_csv(parquet_path: str, csv_path: str):
    logger.info(f"Exporting data from {os.path.basename(parquet_path)} to CSV.")
    df = read_parquet_file(parquet_path)
    df.to_csv(csv_path, sep=';', index=False)
    logger.info(f"Data exported to {csv_path}")

if __name__ == '__main__':
    export_table_to_csv('data/warehouse/course_attendance.parquet', 'data/export/course_attendance.csv')