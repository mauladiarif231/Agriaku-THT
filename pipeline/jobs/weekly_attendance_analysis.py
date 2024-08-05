from pipeline.utils.file_utils import read_parquet_file, write_parquet_file
from pipeline.utils.logger import setup_logging
import pandas as pd

logger = setup_logging('pipeline_log.log')

def weekly_attendance_analysis(attendance_path: str, course_path: str, schedule_path: str, enrollment_path: str, output_path: str):
    logger.info(f"Analyzing weekly attendance.")
    
    # Load data
    try:
        if attendance_path.endswith('.csv'):
            attendance_df = pd.read_csv(attendance_path)
        else:
            attendance_df = read_parquet_file(attendance_path)
        course_df = read_parquet_file(course_path)
        schedule_df = read_parquet_file(schedule_path)
        enrollment_df = read_parquet_file(enrollment_path)
    except Exception as e:
        logger.error(f"Error loading data: {e}")
        raise

    # Debug: Print columns of loaded data
    logger.info(f"Attendance DataFrame columns: {attendance_df.columns}")
    logger.info(f"Course DataFrame columns: {course_df.columns}")
    logger.info(f"Schedule DataFrame columns: {schedule_df.columns}")
    logger.info(f"Enrollment DataFrame columns: {enrollment_df.columns}")

    # Ensure datetime columns are in datetime format
    for col in ['ATTEND_DT', 'SCHEDULE_START_DT', 'SCHEDULE_END_DT']:
        if col in attendance_df.columns:
            try:
                attendance_df[col] = pd.to_datetime(attendance_df[col], errors='coerce')
            except Exception as e:
                logger.error(f"Error converting {col} in attendance_df to datetime: {e}")
                raise
        else:
            logger.error(f"Column {col} is missing in attendance_df")

    if 'SCHEDULE_START_DT' in schedule_df.columns:
        try:
            schedule_df['SCHEDULE_START_DT'] = pd.to_datetime(schedule_df['SCHEDULE_START_DT'], errors='coerce')
            schedule_df['SCHEDULE_END_DT'] = pd.to_datetime(schedule_df['SCHEDULE_END_DT'], errors='coerce')
        except Exception as e:
            logger.error(f"Error converting schedule dates to datetime: {e}")
            raise
    else:
        logger.error("Column SCHEDULE_START_DT or SCHEDULE_END_DT is missing in schedule_df")

    # Merging datasets
    try:
        merged_df = attendance_df.merge(schedule_df, on='SCHEDULE_ID', suffixes=('', '_schedule'))\
                                 .merge(enrollment_df, on='ENROLLMENT_ID', suffixes=('', '_enrollment'))\
                                 .merge(course_df, on='COURSE_ID', suffixes=('', '_course'))
    except Exception as e:
        logger.error(f"Error merging data: {e}")
        raise

    # Debug: Print columns of merged DataFrame
    logger.info(f"Merged DataFrame columns: {merged_df.columns}")

    # Rename columns to avoid ambiguity
    merged_df = merged_df.rename(columns={
        'STUDENT_ID': 'STUDENT_ID',
        'SCHEDULE_START_DT': 'SCHEDULE_START_DT',
        'SCHEDULE_END_DT': 'SCHEDULE_END_DT',
        'COURSE_NAME': 'COURSE_NAME'
    })

    # Calculating WEEK_ID
    try:
        merged_df['WEEK_ID'] = (1 + merged_df['ATTEND_DT'].dt.isocalendar().week - merged_df['SCHEDULE_START_DT'].dt.isocalendar().week)
    except Exception as e:
        logger.error(f"Error calculating WEEK_ID: {e}")
        raise

    # Calculating expected attendance
    try:
        merged_df['ATTENDANCE_EXPECTED'] = merged_df['SCHEDULE_COURSE_DAYS'].apply(lambda x: len(x.split(',')) if pd.notna(x) else 0)
    except Exception as e:
        logger.error(f"Error calculating ATTENDANCE_EXPECTED: {e}")
        raise

    # Grouping and aggregating
    try:
        weekly_attendance_df = merged_df.groupby(['STUDENT_ID', 'SCHEDULE_ID', 'ENROLLMENT_ID', 'WEEK_ID', 'LECTURER_ID', 'SEMESTER_ID', 'COURSE_ID', 'COURSE_NAME', 'ACADEMIC_YEAR', 'ENROLL_DT', 'SCHEDULE_START_DT', 'SCHEDULE_END_DT', 'SCHEDULE_COURSE_DAYS', 'ATTENDANCE_EXPECTED'])['ATTEND_DT'].count().reset_index().rename(columns={'ATTEND_DT': 'ATTENDANCE_SUM'})
    except Exception as e:
        logger.error(f"Error grouping and aggregating data: {e}")
        raise

    # Write output
    try:
        write_parquet_file(weekly_attendance_df, output_path)
    except Exception as e:
        logger.error(f"Error writing output to {output_path}: {e}")
        raise
    
    logger.info(f"Weekly attendance analysis written to {output_path}")

if __name__ == '__main__':
    weekly_attendance_analysis('data/warehouse/daily_attendance.parquet', 
                               'data/warehouse/course_dimension.parquet', 
                               'data/warehouse/schedule_dimension.parquet', 
                               'data/warehouse/enrollment_dimension.parquet', 
                               'data/warehouse/weekly_attendance.parquet')