# pipeline/utils/config.py
import yaml

class Config:
    @staticmethod
    def parse(file_path):
        with open(file_path, 'r') as file:
            config_data = yaml.safe_load(file)
            # Atur atribut berdasarkan config_data
            Config.WAREHOUSE_PATH = config_data.get('WAREHOUSE_PATH', 'data/warehouse')
            Config.EXPORT_DATA_PATH = config_data.get('EXPORT_DATA_PATH', 'data/export')
            Config.RAW_DATA_PATH = config_data.get('RAW_DATA_PATH', 'data/raw')
            Config.FOLDER_DATA = config_data.get('FOLDER_DATA', 'data')

    @staticmethod
    def print_attributes(logger):
        logger.info(f"WAREHOUSE_PATH: {Config.WAREHOUSE_PATH}")
        logger.info(f"EXPORT_DATA_PATH: {Config.EXPORT_DATA_PATH}")
        logger.info(f"RAW_DATA_PATH: {Config.RAW_DATA_PATH}")
        logger.info(f"FOLDER_DATA: {Config.FOLDER_DATA}")

    # Atribut konfigurasi
    FOLDER_DATA = "data"
    WAREHOUSE_PATH = "data/datamart"
    EXPORT_DATA_PATH = "data/export"
    RAW_DATA_PATH = "data/raw"