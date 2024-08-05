from pipeline.utils.file_utils import read_parquet_file, write_parquet_file
from pipeline.utils.logger import setup_logging
import pandas as pd

logger = setup_logging('pipeline_log.log')

def create_attendance_fact(attendance_path: str, enrollment_dimension_path: str, fact_path: str):
    logger.info(f"Creating attendance fact table from {attendance_path}.")
    
    # Load attendance data
    if attendance_path.endswith('.csv'):
        attendance_df = pd.read_csv(attendance_path)
    else:
        attendance_df = read_parquet_file(attendance_path)
    
    # Load enrollment dimension data
    enrollment_df = read_parquet_file(enrollment_dimension_path)
    
    # Convert attendance date to datetime
    try:
        attendance_df['ATTEND_DT'] = pd.to_datetime(attendance_df['ATTEND_DT'], format='%d-%b-%y')  # Adjust format if needed
    except Exception as e:
        logger.error(f"Error converting ATTEND_DT to datetime: {e}")
        raise
    
    # Create the fact table by merging dataframes
    fact_df = attendance_df.rename(columns={'ID': 'ATTENDANCE_ID'}).merge(
        enrollment_df, on=['STUDENT_ID', 'SCHEDULE_ID'], how='left'
    )[['ATTENDANCE_ID', 'STUDENT_ID', 'ENROLLMENT_ID', 'SCHEDULE_ID', 'LECTURER_ID', 'COURSE_ID', 'SEMESTER_ID', 'ATTEND_DT']]
    
    # Write fact table to Parquet
    write_parquet_file(fact_df, fact_path)
    logger.info(f"Attendance fact table written to {fact_path}")

if __name__ == '__main__':
    create_attendance_fact('data/raw/course_attendance.csv', 'data/warehouse/enrollment_dimension.parquet', 'data/warehouse/fact_attendance_daily.parquet')