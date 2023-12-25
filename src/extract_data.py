import pandas as pd
import logging

def extract(filepath):
    """
    Extracts and flattens data from a JSON file.

    Parameters:
    - filepath (str): Path to the JSON file.

    Returns:
    - pd.DataFrame: Extracted and flattened DataFrame.
    """
    try:
        print(f'Extracting Data from {filepath}')
        # Read JSON data from the file
        df = pd.read_json(f'{filepath}')
        # Flatten nested JSON structure
        df = pd.json_normalize(df['transactions'])
        return df
    except Exception as e:
        logging.error(f"An error occurred during data extraction: {str(e)}")
        return pd.DataFrame()  # Return an empty DataFrame in case of an error
