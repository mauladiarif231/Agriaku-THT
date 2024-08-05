# Agriaku-THT

This project is designed to demonstrate the implementation of an ETL (Extract, Transform, Load) process. It involves data ingestion, transformation, and storage using directories as schema and table representations, storing the data in files to simulate a data warehouse.

## Project Description

The Agriaku-THT project showcases an ETL pipeline that processes agricultural data. The pipeline includes the following stages:

1. **Ingestion:** Collect raw data from various sources.
2. **Transformation:** Clean and process the data to fit the desired schema.
3. **Storage:** Store the transformed data in a file-based data warehouse.

## Prerequisites

To run this project, you will need the following:

- Python 3.x
- Required Python packages (listed in `requirements.txt`)

## Installation

1. **Clone the repository:**
   ```bash
   git clone https://github.com/mauladiarif231/Agriaku-THT.git
   ```
2. **Navigate to the project directory:**
   ```bash
   cd Agriaku-THT
   ```
3. **Install the required packages:**
   ```bash
   pip install -r requirements.txt
   ```
## Project Structure

```
Agriaku-THT/
├── data/
│   ├── raw/                        # Raw data files
│   ├── export/                     # Exported data files
├── logs/                           # Logs for pipeline execution
├── pipeline/
│   ├── jobs/                       # ETL job scripts
│   │   ├── create_attendance_fact.py     # Create attendance fact table
│   │   ├── create_course_dimension.py    # Create course dimension table
│   │   ├── create_enrollment_dimension.py # Create enrollment dimension table
│   │   ├── create_schedule_dimension.py  # Create schedule dimension table
│   │   ├── export_data.py                # Export data to files
│   │   ├── ingest_data.py                # Ingest raw data
│   │   ├── weekly_attendance_analysis.py # Analyze weekly attendance
│   │   ├── weekly_attendance_summary.py  # Summarize weekly attendance
│   ├── utils/                      # Utility functions
│   │   ├── config.py               # Configuration utilities
│   │   ├── file_utils.py           # File handling utilities
│   │   ├── logger.py               # Logging utilities
│   └── __init__.py                 # Package initialization
├── pipeline_conf.yml               # Configuration for the pipeline
├── Dockerfile                      # Docker configuration
├── README.md                       # Project documentation
├── docker-compose.yml              # Docker Compose configuration
├── pipeline.py                     # Main pipeline script
├── requirements.txt                # Python package dependencies
└── run.sh                          # Script to run the project
```
---

### Running the Pipeline on Local Machine

Please make sure that you are inside the repository and have already activated the environment before running the pipeline. Setup and installation steps can be found above.

To run the pipeline, use the following command:

```bash
$ python3 pipeline.py
```

The warehouse tables can be found inside the `data/analytics`, `data/datamart`, `data/staging` directory, while the final CSV files can be found inside `data/export/`.

```
.
├── data
│   ├── raw
│   │   ├── course_attendance.csv
│   │   ├── course.csv
│   │   ├── enrollment.csv
│   │   └── schedule.csv
│   ├── export
│   │   ├── analytics_attendance_summary_weekly.csv
│   │   ├── analytics_attendance_weekly.csv
│   │   ├── dim_course.csv
│   │   ├── dim_enrollment.csv
│   │   ├── dim_schedule.csv
│   │   └── fact_attendance_daily.csv
│   ├── analytics
│   │   ├── analytics_attendance_summary_weekly.parquet
│   │   └── analytics_attendance_weekly.parquet
│   ├── datamart
│   │   ├── dim_course.parquet
│   │   ├── dim_enrollment.parquet
│   │   ├── dim_schedule.parquet
│   │   └── fact_attendance_daily.parquet
│   └── staging
│       ├── stg_course_attendance.parquet
│       ├── stg_course.parquet
│       ├── stg_enrollment.parquet
│       ├── stg_schedule.parquet
├── pipeline/
├── pipeline_conf.yml
├── Dockerfile
├── README.md
├── docker-compose.yml
├── pipeline.py
├── requirements.txt
└── run.sh                         
```

### Running with Docker

Before running with Docker, please make sure that the datasets are inside the `data/raw/` directory as shown in the directory tree above.

1. **Build Docker Image & Run Docker Container:**
   - Make sure Docker is installed on your machine.
   - Navigate to the project directory and build the Docker image using the prepared script.
   - Use the provided `run.sh` script to run the Docker container and execute the ETL pipeline:

     ```bash
     $ ./run.sh
     ```

You should be able to find the results inside the `docker/` directory that has been created by the run script.

```
.
├── data
│   ├── raw
│   │   ├── course_attendance.csv
│   │   ├── course.csv
│   │   ├── enrollment.csv
│   │   └── schedule.csv
├── docker/
│   ├── analytics
│   │   │   ├── analytics_attendance_summary_weekly.parquet
│   │   │   └── analytics_attendance_weekly.parquet
│   ├── datamart
│   │   │   ├── dim_course.parquet
│   │   │   ├── dim_enrollment.parquet
│   │   │   ├── dim_schedule.parquet
│   │   │   └── fact_attendance_daily.parquet
│   ├── exported-data
│   │   ├── analytics_attendance_summary_weekly.csv
│   │   ├── analytics_attendance_weekly.csv
│   │   ├── dim_course.csv
│   │   ├── dim_enrollment.csv
│   │   ├── dim_schedule.csv
│   │   └── fact_attendance_daily.csv
│   ├── staging
│   │       ├── stg_course_attendance.parquet
│   │       ├── stg_course.parquet
│   │       ├── stg_enrollment.parquet
│   │       └── stg_schedule.parquet
├── pipeline/
├── pipeline_conf.yml
├── Dockerfile
├── README.md
├── docker-compose.yml
├── pipeline.py
├── requirements.txt
└── run.sh     
```

These instructions provide a clear guide for running the ETL pipeline both on a local machine and within a Docker container, ensuring users can choose their preferred method.

## License

This project is licensed under the MIT License. See the [LICENSE](LICENSE) file for details.

---

Feel free to modify this template to suit your specific needs and add more details if necessary. Once you've made any adjustments, you can save it as `README.md` in your project's root directory.
