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

## Usage

To run the ETL pipeline, follow these steps:

1. **Ingest Data:**
   - Place the raw data files in the `data/raw` directory.
   - Run the ingestion script to load the data.
   ```bash
   python scripts/ingest.py
   ```

2. **Transform Data:**
   - Run the transformation script to clean and process the data.
   ```bash
   python scripts/transform.py
   ```

3. **Store Data:**
   - The processed data will be stored in the `data/staging` and `data/datamart` directories.

4. **Additional Information:**
   - You can find the details of the schema and data structure in the `docs` directory.

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

## Contributing

If you would like to contribute to this project, please fork the repository and submit a pull request.

## License

This project is licensed under the MIT License. See the [LICENSE](LICENSE) file for details.

---

Feel free to modify this template to suit your specific needs and add more details if necessary. Once you've made any adjustments, you can save it as `README.md` in your project's root directory.
