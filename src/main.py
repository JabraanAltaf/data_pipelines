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
    """
    Read a configuration option from the specified section in the config file.

    Parameters:
    - section (str): The section in the configuration file.
    - option (str): The option to retrieve from the specified section.

    Returns:
    str: The value of the specified option.
    """
    config = configparser.ConfigParser()
    config.read('../config.ini')
    return config.get(section, option)

def main():
    """
    Main function to process data files based on configuration settings.

    This function reads file paths from the configuration file, extracts data,
    performs quality checks, and processes the data into the Postgres Database.
    """
    data_filepaths = read_config('main', 'data_filepaths').split(', ')
    
    # Run the pipeline for every data file
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
