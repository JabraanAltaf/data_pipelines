import logging
import psycopg2
import pandas as pd
from configparser import ConfigParser
from read_config import read_config
from db_connection import create_connection

def create_error_log_table(cursor):
    """
    Creates the error log table if it doesn't exist.

    Parameters:
    - cursor: Database cursor.
    """
    try:
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS error_logging (
                transactionId TEXT PRIMARY KEY,
                customerId TEXT,
                failed_currency_check BOOLEAN,
                failed_date_check BOOLEAN,
                duplicated_row BOOLEAN
            );
        """)

    except Exception as e:
        logging.error(f"An error occurred while creating the error log table: {str(e)}")

def insert_error_log(cursor, error_df):
    """
    Inserts failed transactions into the error log table.

    Parameters:
    - cursor: Database cursor.
    - error_df (pd.DataFrame): DataFrame with failed transactions and error details.
    """
    try:
        for index, row in error_df.iterrows():
            cursor.execute("""
                INSERT INTO error_logging (transactionId, customerId, failed_currency_check, failed_date_check, duplicated_row)
                VALUES (%s, %s, %s, %s, %s)
                ON CONFLICT (transactionId) DO NOTHING;
            """, (
                row["transactionId"],
                row["customerId"],
                row["failed_currency_check"],
                row["failed_date_check"],
                row["duplicated_row"]
            ))
        print("Insertion successful.")
    except Exception as e:
        print("insert_error")
        logging.error(f"An error occurred while inserting into the error log table: {str(e)}")

def write_error_log(error_df):
    """
    Writes failed transactions to the error log table in PostgreSQL.

    Parameters:
    - error_df (pd.DataFrame): DataFrame with failed transactions and error details.
    """
    conn = create_connection()
    cursor = conn.cursor()
    # Create or ensure the existence of the error log table
    create_error_log_table(cursor)
    # Insert failed transactions into the error log table
    insert_error_log(cursor, error_df)
    # Commit changes to the database
    conn.commit()
