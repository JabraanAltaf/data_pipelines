import logging 
import psycopg2
from read_config import read_config

def create_connection():
    """
    Creates a connection to the PostgreSQL database.

    Returns:
    - psycopg2.extensions.connection: Database connection.
    """
    try:
        print('Establishing Connection with Database')
        return psycopg2.connect(
            host=read_config('postgres', 'host'),
            database=read_config('postgres', 'database'),
            user=read_config('postgres', 'user'),
            password=read_config('postgres', 'password')
        )
    except Exception as e:
        logging.error(f"An error occurred while creating a database connection: {str(e)}")
        return None  # Return None in case of an error
