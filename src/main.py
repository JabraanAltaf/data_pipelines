import logging
from extract_data import extract
from data_quality_check import quality_check
from data_processing import process_data
import configparser

logging.basicConfig(
    filename='app.log',
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] - %(message)s',
)

def read_config(section, option):
    config = configparser.ConfigParser()
    config.read('../config.ini')
    return config.get(section, option)

def main():
    data_filepaths = read_config('main', 'data_filepaths').split(', ')
    
    for data_filepath in data_filepaths:
        print('--------------------------------------------')
        print(f'Processing File: {data_filepath}')
        data = extract(data_filepath)
        passed_data = quality_check(data)
        process_data(passed_data)
        print(f'Completed Processing File: {data_filepath}')
        print('--------------------------------------------')
    

if __name__ == "__main__":
    main()
