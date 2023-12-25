import logging
import psycopg2
import pandas as pd
from configparser import ConfigParser
from read_config import read_config
from db_connection import create_connection

def create_customers_table(cursor):
    print('Creating Customers table if does not exist')
    """
    Creates the 'customers' table if it doesn't exist.

    Parameters:
    - cursor: Database cursor.
    """
    sql_create_customers = """
        CREATE TABLE IF NOT EXISTS customers (
            customerId TEXT PRIMARY KEY,
            mostRecentTransactionDate DATE,
            sourceDate DATE
        )
    """
    cursor.execute(sql_create_customers)

def create_transactions_table(cursor):
    print('Creating Transactions Table if does not exist')
    """
    Creates the 'transactions' table if it doesn't exist.

    Parameters:
    - cursor: Database cursor.
    """
    sql_create_transactions = """
        CREATE TABLE IF NOT EXISTS transactions (
            transactionId TEXT PRIMARY KEY,
            customerId TEXT REFERENCES customers(customerId),
            transactionDate DATE,
            sourceDate DATE,
            merchantId INTEGER,
            categoryId INTEGER,
            currency TEXT,
            amount DECIMAL,
            description TEXT,
            UNIQUE (customerId, transactionId)
        );
    """
    cursor.execute(sql_create_transactions)

def upsert_customers(cursor, df):
    print('Upsert Verified Data into Customers Table')
    """
    Upserts customer data into the 'customers' table.

    Parameters:
    - cursor: Database cursor.
    - df (pd.DataFrame): DataFrame with customer data.
    """
    for index, row in df.iterrows():
        cursor.execute("""
            INSERT INTO customers (customerId, mostRecentTransactionDate, sourceDate)
            VALUES (%s, %s, %s)
            ON CONFLICT (customerId) DO UPDATE
            SET mostRecentTransactionDate = GREATEST(EXCLUDED.mostRecentTransactionDate, customers.mostRecentTransactionDate)
            WHERE customers.sourceDate < EXCLUDED.sourceDate;
        """, (
            row["customerId"],
            row["transactionDate"],
            row["sourceDate"]
        ))
    print("Upsert completed.")

def upsert_transactions(cursor, df):
    print('Upsert Verified Data into Transactions Table')
    """
    Upserts transaction data into the 'transactions' table.

    Parameters:
    - cursor: Database cursor.
    - df (pd.DataFrame): DataFrame with transaction data.
    """
    for index, row in df.iterrows():
        cursor.execute("""
            INSERT INTO transactions (transactionId, customerId, transactionDate, currency, amount, sourceDate,
                                              merchantId, categoryId, description)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (transactionId) DO UPDATE
            SET customerId = EXCLUDED.customerId,
                transactionDate = EXCLUDED.transactionDate,
                currency = EXCLUDED.currency,
                amount = EXCLUDED.amount,
                sourceDate = EXCLUDED.sourceDate,
                merchantId = EXCLUDED.merchantId,
                categoryId = EXCLUDED.categoryId,
                description = EXCLUDED.description
            WHERE transactions.sourceDate < EXCLUDED.sourceDate;
        """, (
            row["transactionId"],
            row["customerId"],
            row["transactionDate"],
            row["currency"],
            row["amount"],
            row["sourceDate"],
            row["merchantId"],
            row["categoryId"],
            row["description"]
        ))
    print("Upsert completed.")

def process_data(df):
    """
    Orchestrates the entire data processing workflow.

    Parameters:
    - df (pd.DataFrame): Input DataFrame with transaction data.
    """
    # Establish connection with Database
    conn = create_connection()
    cursor = conn.cursor()
    # Create the tables if they don't exist
    create_customers_table(cursor)
    create_transactions_table(cursor)
    # Upsert data into the two tables
    upsert_customers(cursor, df)
    upsert_transactions(cursor, df)
    # Commit changes to the database
    conn.commit()

