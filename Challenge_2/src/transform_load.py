import pandas as pd
import logging
import os
from datetime import datetime

# Get the directory of the current script
script_dir = os.path.dirname(os.path.abspath(__name__))

# Configure logging to save logs in the "logs" folder next to the "src" folder
date_stamp = datetime.now().strftime("%Y%m%d%H%M%S")
log_dir = r'Challenge_2/logs' #os.path.join(script_dir, 'Challenge_2', 'logs')
os.makedirs(log_dir, exist_ok=True)
logging.basicConfig(
    filename=os.path.join(log_dir, f'{date_stamp}_etl_transform.log'),
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)

# File paths
path_prefix = 'Challenge_2/data'

deposits_raw =  f'{path_prefix}/raw/raw_deposits.csv'
events_raw =  f'{path_prefix}/raw/raw_events.csv'
users_raw = f'{path_prefix}/raw/raw_users.csv'
withdrawals_raw = f'{path_prefix}/raw/raw_withdrawals.csv'

# Read the CSV files from the RAW stage
try:
    deposits = pd.read_csv(deposits_raw, sep=';')
    events = pd.read_csv(events_raw, sep=';')
    users = pd.read_csv(users_raw, sep=';')
    withdrawals = pd.read_csv(withdrawals_raw, sep=';')
    logging.info("CSV files read successfully.")

except Exception as e:
    logging.error(f"Error reading CSV files: {e}")
    raise

 # Create dim_users table
try:
    dim_users = users[['user_id']]
    dim_users['_analytics_insert_date'] = datetime.now()
    logging.info("dim_users table created successfully.")

except Exception as e:
    logging.error(f"Error creating dim_users table: {e}")
    raise

# Create dim_dates table
try:
    dates = pd.concat([deposits['event_timestamp'], withdrawals['event_timestamp']]).dropna().unique()
    dim_dates = pd.DataFrame({'date': pd.to_datetime(dates)})
    dim_dates['year'] = dim_dates['date'].dt.year
    dim_dates['month'] = dim_dates['date'].dt.month
    dim_dates['day'] = dim_dates['date'].dt.day
    dim_dates['weekday'] = dim_dates['date'].dt.weekday
    dim_dates['hour'] = dim_dates['date'].dt.hour
    dim_dates['minute'] = dim_dates['date'].dt.minute
    dim_dates['date_id'] = range(1, len(dim_dates) + 1)
    dim_dates['_analytics_insert_date'] = datetime.now()
    logging.info("dim_dates table created successfully.")

except Exception as e:
    logging.error(f"Error creating dim_dates table: {e}")
    raise

# Create fact_transactions table
try:
    deposits['transaction_type'] = 'deposit'
    withdrawals['transaction_type'] = 'withdrawal'
    fact_transactions = pd.concat([deposits, withdrawals])
    fact_transactions['event_timestamp'] = pd.to_datetime(fact_transactions['event_timestamp'], errors='coerce', utc=True)
    fact_transactions = fact_transactions[
        ['id', 'user_id', 'event_timestamp', 'amount', 'currency', 'transaction_type', 'tx_status', 'interface']
    ]
    fact_transactions.columns = ['transaction_id', 'user_id', 'event_timestamp', 'amount', 'currency', 'transaction_type', 'tx_status', 'interface']
    fact_transactions['_analytics_insert_date'] = datetime.now()
    logging.info("fact_transactions table created successfully.")

except Exception as e:
    logging.error(f"Error creating fact_transactions table: {e}")
    raise

# Create fact_events table
try:
    fact_events = events[['id', 'event_timestamp', 'user_id', 'event_name']]
    fact_events['_analytics_insert_date'] = datetime.now()
    logging.info("fact_events table created successfully.")

except Exception as e:
    logging.error(f"Error creating fact_events table: {e}")
    raise

# Save the tables in the Analytics stage
try:
    output_dir = os.path.join(path_prefix, 'analytics')
    os.makedirs(output_dir, exist_ok=True)
    dim_users.to_csv(os.path.join(output_dir, 'dim_users.csv'), sep=';', index=False)
    dim_dates.to_csv(os.path.join(output_dir, 'dim_dates.csv'), sep=';', index=False)
    fact_transactions.to_csv(os.path.join(output_dir, 'fact_transactions.csv'), sep=';', index=False)
    fact_events.to_csv(os.path.join(output_dir, 'fact_events.csv'), sep=';', index=False)
    logging.info("Tables saved to CSV successfully.")

except Exception as e:
    logging.error(f"Error saving tables to CSV: {e}")
    raise