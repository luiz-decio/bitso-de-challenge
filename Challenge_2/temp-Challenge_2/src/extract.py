import pandas as pd
import os
import logging
from datetime import datetime

# Define the date to save the files
date_stamp = datetime.now().strftime("%Y%m%d%H%M%S")

# Configure logging
log_dir = "/usr/local/airflow/logs/etl_finance"
os.makedirs(log_dir, exist_ok=True)
logging.basicConfig(
    filename=os.path.join(log_dir, f'{date_stamp}_{__name__}.log'),
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)

# File paths
path_prefix = '/usr/local/airflow/data'

deposit_file =  f'{path_prefix}/landing/deposit_sample_data.csv'
event_file =  f'{path_prefix}/landing/event_sample_data.csv'
user_file = f'{path_prefix}/landing/user_id_sample_data.csv'
withdrawal_file = f'{path_prefix}/landing/withdrawals_sample_data.csv'

# Read the data from landing stage

try:
    deposit_data = pd.read_csv(deposit_file, delimiter=',')
    logging.info("Deposit data read successfully.")
except Exception as e:
    logging.error(f"Error reading deposit data: {e}")

try:
    event_data = pd.read_csv(event_file, delimiter=',')
    logging.info("Event data read successfully.")
except Exception as e:
    logging.error(f"Error reading event data: {e}")

try:
    user_data = pd.read_csv(user_file)
    logging.info("User data read successfully.")
except Exception as e:
    logging.error(f"Error reading user data: {e}")

try:
    withdrawal_data = pd.read_csv(withdrawal_file, delimiter=',')
    logging.info("Withdrawal data read successfully.")
except Exception as e:
    logging.error(f"Error reading withdrawal data: {e}")


'''
    Normalize column names and data formats
    - Standardize string columns
    - Rename columns for clarity
    - Format date columns to datetime
    - Add column with isert date
'''

try:
    # Deposit Data
    deposit_data.columns = deposit_data.columns.str.strip().str.lower()
    deposit_data['event_timestamp'] = pd.to_datetime(deposit_data['event_timestamp'], errors='coerce', utc=True)
    deposit_data['amount'] = pd.to_numeric(deposit_data['amount'], errors='coerce')
    deposit_data['_raw_insert_date'] = datetime.now()
    
    logging.info("Deposit data normalized successfully.")

    # Event Data
    event_data.columns = event_data.columns.str.strip().str.lower()
    event_data['event_timestamp'] = pd.to_datetime(event_data['event_timestamp'], errors='coerce', utc=True)
    event_data['_raw_insert_date'] = datetime.now()
    
    logging.info("Event data normalized successfully.")

    # User Data
    user_data.columns = user_data.columns.str.strip().str.lower()
    user_data['_raw_insert_date'] = datetime.now()
    
    logging.info("User data normalized successfully.")

    # Withdrawal Data
    withdrawal_data.columns = withdrawal_data.columns.str.strip().str.lower()
    withdrawal_data['event_timestamp'] = pd.to_datetime(withdrawal_data['event_timestamp'], errors='coerce', utc=True)
    withdrawal_data['amount'] = pd.to_numeric(withdrawal_data['amount'], errors='coerce')
    withdrawal_data['_raw_insert_date'] = datetime.now()
    
    logging.info("Withdrawal data normalized successfully.")

except Exception as e:
    logging.error(f"Error normalizing data: {e}")

'''
    Save the data transformed in the raw stage
'''

try:
    # Save the data transformed in the raw stage
    deposit_data.to_csv(f'{path_prefix}/raw/raw_deposits.csv', sep=';', index=False)
    logging.info("Deposit data saved successfully.")

    event_data.to_csv(f'{path_prefix}/raw/raw_events.csv', sep=';', index=False)
    logging.info("Event data saved successfully.")

    user_data.to_csv(f'{path_prefix}/raw/raw_users.csv', sep=';', index=False)
    logging.info("User data saved successfully.")

    withdrawal_data.to_csv(f'{path_prefix}/raw/raw_withdrawals.csv', sep=';', index=False)
    logging.info("Withdrawal data saved successfully.")
    
except Exception as e:
    logging.error(f"Error saving raw data: {e}")