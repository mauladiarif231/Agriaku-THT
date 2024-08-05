from pipeline.utils.file_utils import read_parquet_file, write_parquet_file
from pipeline.utils.logger import setup_logging

logger = setup_logging('pipeline_log.log')

def create_course_dimension(staging_path: str, dimension_path: str):
    logger.info(f"Creating course dimension from {staging_path}.")
    df = read_parquet_file(staging_path)
    df = df.rename(columns={'ID': 'COURSE_ID', 'NAME': 'COURSE_NAME'})
    write_parquet_file(df, dimension_path)
    logger.info(f"Course dimension written to {dimension_path}")

if __name__ == '__main__':
    create_course_dimension('data/staging/course.parquet', 'data/warehouse/course_dimension.parquet')