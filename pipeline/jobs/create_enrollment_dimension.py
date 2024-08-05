from pipeline.utils.file_utils import read_parquet_file, write_parquet_file
from pipeline.utils.logger import setup_logging
import pandas as pd

logger = setup_logging('pipeline_log.log')

def create_enrollment_dimension(staging_path: str, course_dimension_path: str, schedule_dimension_path: str, dimension_path: str):
    logger.info(f"Creating enrollment dimension from {staging_path}.")
    
    # Load staging and dimension tables
    df = read_parquet_file(staging_path)
    course_df = read_parquet_file(course_dimension_path)
    schedule_df = read_parquet_file(schedule_dimension_path)
    
    # Convert enrollment date to datetime with error handling
    try:
        df['ENROLL_DT'] = pd.to_datetime(df['ENROLL_DT'], format='%d-%b-%y', errors='coerce')  # Updated format
    except Exception as e:
        logger.error(f"Error converting ENROLL_DT to datetime: {e}")
        raise
    
    # Merge dataframes
    merged_df = df.rename(columns={'ID': 'ENROLLMENT_ID', 'SEMESTER': 'SEMESTER_ID'})\
                  .merge(schedule_df, on='SCHEDULE_ID', how='left')\
                  .merge(course_df, on='COURSE_ID', how='left')
    
    # Select relevant columns
    merged_df = merged_df[['ENROLLMENT_ID', 'SCHEDULE_ID', 'STUDENT_ID', 'LECTURER_ID', 'COURSE_ID', 'ACADEMIC_YEAR', 'SEMESTER_ID', 'ENROLL_DT']]
    
    # Write merged dataframe to warehouse
    write_parquet_file(merged_df, dimension_path)
    logger.info(f"Enrollment dimension written to {dimension_path}")

if __name__ == '__main__':
    create_enrollment_dimension(
        'data/staging/enrollment.parquet',
        'data/warehouse/course_dimension.parquet',
        'data/warehouse/schedule_dimension.parquet',
        'data/warehouse/enrollment_dimension.parquet'
    )