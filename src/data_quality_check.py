import logging
import pandas as pd
from error_handling import create_error_log_table, insert_error_log, write_error_log

def quality_check(df):
    """
    Performs data quality checks on the DataFrame.

    Parameters:
    - df (pd.DataFrame): Input DataFrame.

    Returns:
    - pd.DataFrame: DataFrame with passed quality checks.
    """
    try:
        print('Running Data Quality Checks')
        currencies = ["EUR", "USD", "GBP"]
        # Data Integrity Check - Filter transactions by the allowed currencies
        df_passed = df[df['currency'].isin(currencies)]

        # Check for invalid transactionDate
        df_passed['transactionDate'] = pd.to_datetime(df_passed['transactionDate'], errors='coerce')
        df_passed = df_passed.dropna(subset=['transactionDate'])

        # Check for duplicate transaction records
        df_passed = df_passed.drop_duplicates()
        # Identify failed transactions
        df_failed = df[~df.index.isin(df_passed.index)]

        # Identify the DQ checks that failed for each row
        error_df = pd.DataFrame(index=df_failed.index)
        error_df["transactionId"] = df_failed["transactionId"]
        error_df["customerId"] = df_failed["customerId"]
        error_df["failed_currency_check"] = ~df_failed['currency'].isin(currencies)
        error_df["failed_date_check"] = pd.to_datetime(df_failed['transactionDate'], errors='coerce').isna()
        error_df["duplicated_row"] = df_failed.duplicated()

        # Write failed transactions to the error log table
        print('Writing Failed Checks to Error Log Table')
        write_error_log(error_df)

        return df_passed
    except Exception as e:
        print("errors")
        logging.debug(f"An error occurred during data quality check: {str(e)}")
        return pd.DataFrame()  # Return an empty DataFrame in case of an error
