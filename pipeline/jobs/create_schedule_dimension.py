from pipeline.utils.file_utils import read_parquet_file, write_parquet_file
from pipeline.utils.logger import setup_logging
import pandas as pd

logger = setup_logging('pipeline_log.log')

def create_schedule_dimension(staging_table_path: str, output_path: str):
    logger.info(f"Creating schedule dimension from {output_path}.")
    
    df = read_parquet_file(staging_table_path)
    df['START_DT'] = pd.to_datetime(df['START_DT'], format='%d-%b-%y')
    df['END_DT'] = pd.to_datetime(df['END_DT'], format='%d-%b-%y')
    
    df = df.rename(columns={
        'ID': 'SCHEDULE_ID',
        'START_DT': 'SCHEDULE_START_DT',
        'END_DT': 'SCHEDULE_END_DT',
        'COURSE_DAYS': 'SCHEDULE_COURSE_DAYS'
    })
    
    write_parquet_file(df, output_path)
    logger.info(f"Schedule dimension written to {output_path}")

if __name__ == '__main__':
    create_schedule_dimension('data/staging/schedule.parquet', 'data/warehouse/schedule_dimension.parquet')