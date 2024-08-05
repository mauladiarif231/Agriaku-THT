# pipeline.py
from pipeline.jobs.ingest_data import ingest_raw_csv_table
from pipeline.jobs.export_data import export_table_to_csv

from pipeline.jobs.create_course_dimension import create_course_dimension
from pipeline.jobs.create_schedule_dimension import create_schedule_dimension
from pipeline.jobs.create_enrollment_dimension import create_enrollment_dimension
from pipeline.jobs.create_attendance_fact import create_attendance_fact
from pipeline.jobs.weekly_attendance_analysis import weekly_attendance_analysis
from pipeline.jobs.weekly_attendance_summary import weekly_attendance_summary

from pipeline.utils.config import Config
from pipeline.utils.logger import setup_logging

import os

# Setup Logging
logger = setup_logging('pipeline_log.log')

def run_pipeline_flows(raw_data_dir: str, export_data_dir: str):
    # INGESTIONS
    r_course_table_path = os.path.join(raw_data_dir, "course.csv")
    r_course_attendance_table_path = os.path.join(raw_data_dir, "course_attendance.csv")
    r_enrollment_table_path = os.path.join(raw_data_dir, "enrollment.csv")
    r_schedule_table_path = os.path.join(raw_data_dir, "schedule.csv")

    staging_path = os.path.join(Config.FOLDER_DATA, "staging")
    datamart_path = os.path.join(Config.FOLDER_DATA, "datamart")
    analytics_path = os.path.join(Config.FOLDER_DATA, "analytics") 

    os.makedirs(staging_path, exist_ok=True)
    os.makedirs(datamart_path, exist_ok=True)
    os.makedirs(analytics_path, exist_ok=True) 

    ingest_raw_csv_table(r_course_table_path, os.path.join(staging_path, "stg_course.parquet"))
    ingest_raw_csv_table(r_course_attendance_table_path, os.path.join(staging_path, "stg_course_attendance.parquet"))
    ingest_raw_csv_table(r_enrollment_table_path, os.path.join(staging_path, "stg_enrollment.parquet"))
    ingest_raw_csv_table(r_schedule_table_path, os.path.join(staging_path, "stg_schedule.parquet"))

    # TRANSFORMATIONS
    dim_course_path = os.path.join(datamart_path, "dim_course.parquet")
    create_course_dimension(os.path.join(staging_path, "stg_course.parquet"), dim_course_path)

    dim_schedule_path = os.path.join(datamart_path, "dim_schedule.parquet")
    create_schedule_dimension(os.path.join(staging_path, "stg_schedule.parquet"), dim_schedule_path)

    dim_enrollment_path = os.path.join(datamart_path, "dim_enrollment.parquet")
    create_enrollment_dimension(os.path.join(staging_path, "stg_enrollment.parquet"), dim_course_path, dim_schedule_path, dim_enrollment_path)

    fact_attendance_daily_path = os.path.join(datamart_path, "fact_attendance_daily.parquet")
    create_attendance_fact(os.path.join(staging_path, "stg_course_attendance.parquet"), dim_enrollment_path, fact_attendance_daily_path)

    analytics_attendance_weekly_path = os.path.join(analytics_path, 'analytics_attendance_weekly.parquet')
    weekly_attendance_analysis(fact_attendance_daily_path, dim_course_path, dim_schedule_path, dim_enrollment_path, analytics_attendance_weekly_path)

    analytics_attendance_summary_weekly_path = os.path.join(analytics_path, 'analytics_attendance_summary_weekly.parquet')
    weekly_attendance_summary(analytics_attendance_weekly_path, analytics_attendance_summary_weekly_path)

    # EXPORTS
    tables_to_export = [
        dim_course_path,
        dim_schedule_path,
        dim_enrollment_path,
        fact_attendance_daily_path,
        analytics_attendance_weekly_path,
        analytics_attendance_summary_weekly_path,
    ]

    for table_path in tables_to_export:
        base_name = os.path.basename(table_path).replace('.parquet', '.csv')
        target_path = os.path.join(export_data_dir, base_name)
        export_table_to_csv(table_path, target_path)

def prepare_pipeline():
    logger.info("Loading configuration")
    Config.parse('pipeline_conf.yml')
    logger.info("Running pipeline with the following config")
    Config.print_attributes(logger)

    logger.info("Preparing required directories")
    os.makedirs(Config.WAREHOUSE_PATH, exist_ok=True)
    os.makedirs(Config.EXPORT_DATA_PATH, exist_ok=True)

    raw_data_staging_path = os.path.join(Config.FOLDER_DATA, 'staging')
    raw_data_datamart_path = os.path.join(Config.FOLDER_DATA, 'datamart')

    os.makedirs(raw_data_staging_path, exist_ok=True)
    os.makedirs(raw_data_datamart_path, exist_ok=True)

if __name__ == "__main__":
    logger.info("Pipeline process started.")
    prepare_pipeline()
    logger.info(f"Raw data directory: {Config.RAW_DATA_PATH}")
    logger.info(f"Export data directory: {Config.EXPORT_DATA_PATH}")
    run_pipeline_flows(Config.RAW_DATA_PATH, Config.EXPORT_DATA_PATH)