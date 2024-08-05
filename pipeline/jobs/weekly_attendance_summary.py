from pipeline.utils.file_utils import read_parquet_file, write_parquet_file
from pipeline.utils.logger import setup_logging

logger = setup_logging('pipeline_log.log')

def weekly_attendance_summary(input_path: str, output_path: str):
    logger.info(f"Summarizing weekly attendance from {input_path}.")
    
    # Load the data
    try:
        df = read_parquet_file(input_path)
        logger.info(f"Loaded DataFrame with columns: {df.columns}")
        logger.info(f"DataFrame preview:\n{df.head()}")
    except Exception as e:
        logger.error(f"Error loading data from {input_path}: {e}")
        return

    # Check if required columns are present
    required_columns = ['SEMESTER_ID', 'WEEK_ID', 'COURSE_ID', 'COURSE_NAME', 'ATTENDANCE_SUM', 'ATTENDANCE_EXPECTED']
    missing_columns = [col for col in required_columns if col not in df.columns]
    
    if missing_columns:
        logger.error(f"Missing columns in DataFrame: {missing_columns}")
        return
    
    # Perform the aggregation
    try:
        summary_df = df.groupby(['SEMESTER_ID', 'WEEK_ID', 'COURSE_ID', 'COURSE_NAME']).agg({
            'ATTENDANCE_SUM': 'sum',
            'ATTENDANCE_EXPECTED': 'sum'
        }).reset_index()
        
        logger.info(f"Aggregated DataFrame preview:\n{summary_df.head()}")
        
        # Calculate attendance percentage
        summary_df['ATTENDANCE_PCT'] = (summary_df['ATTENDANCE_SUM'] / summary_df['ATTENDANCE_EXPECTED']) * 100
        
        # Keep only the necessary columns
        summary_df = summary_df[['SEMESTER_ID', 'WEEK_ID', 'COURSE_NAME', 'ATTENDANCE_PCT']]
        
        logger.info(f"Filtered DataFrame with selected columns:\n{summary_df.head()}")
    except Exception as e:
        logger.error(f"Error during aggregation or calculation: {e}")
        return

    # Write the summarized data to the output file
    try:
        write_parquet_file(summary_df, output_path)
        logger.info(f"Weekly attendance summary written to {output_path}")
    except Exception as e:
        logger.error(f"Error writing data to {output_path}: {e}")

if __name__ == '__main__':
    weekly_attendance_summary('data/warehouse/weekly_attendance.parquet', 'data/warehouse/weekly_attendance_summary.parquet')