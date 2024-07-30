import requests
import schedule
import os
import time
import hmac
import hashlib
import json
import logging
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from datetime import datetime
from dotenv import load_dotenv

# Define the date to save the files
date_stamp = datetime.now().strftime("%Y%m%d%H%M%S")

# Configure logging
log_dir = "/usr/local/airflow/logs/etl_order_books"
os.makedirs(log_dir, exist_ok=True)
logging.basicConfig(
    filename=os.path.join(log_dir, f'{date_stamp}_etl_order_books.log'),
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)

# Load the API key and secret
load_dotenv(os.path.join(os.getcwd(), 'Challenge_1', 'src', '.env'))

API_KEY = os.getenv('API_KEY')
API_SECRET = os.getenv("API_SECRET")

def get_order_book(book: str) -> dict:
    base_url = "https://sandbox.bitso.com"
    request_path = f"/api/v3/order_book/?book={book}"

    logging.info(f"Fetching order book for {book}")
    response = requests.get(f"{base_url}{request_path}")

    if response.status_code == 200:
        logging.info(f"Successfully fetched order book for {book}")
        return response.json()
    else:
        logging.error(f"Failed to fetch data: {response.status_code} {response.text}")
        raise Exception(f"Failed to fetch data: {response.status_code} {response.text}")
    
# Define a function to gather the 600 observation from 10 minutes of orders
def gather_observations(seconds: int, book: str):

    observations = []

    logging.info(f"Gathering {seconds} observations for {book}")
    for i in range(seconds):

        # Get the start time
        start_time = time.time()

        # Gather the observation from that second and add to the list
        observation = get_order_book(book)
        observations.append(observation)

        # Calculate the time to sleep to ensure the next interaction starts exactly 1 second later
        elapsed_time = time.time() - start_time
        sleep_time = max(0, 1 - elapsed_time)
        
        # Sleep for the required time
        time.sleep(sleep_time)
    
    logging.info(f"Successfully gathered observations for {book}")
    return observations

def create_records_dataframe(observations: list, book: str):
    records = []

    logging.info(f"Creating records dataframe for {book}")
    try:
        for observation in observations:

            bids = observation['payload']['bids']
            asks = observation['payload']['asks']

            # Find the best bid and best ask and convert to float
            best_bid = max(float(bid['price']) for bid in bids)
            best_ask = min(float(ask['price']) for ask in asks)

            # Calculate the spread
            spread = (best_ask - best_bid) * 100 / best_ask
            
            record = {
                'orderbook_timestamp': str(observation['payload']['updated_at']), 
                'book': book,
                'bid' :best_bid,
                'ask': best_ask, 
                'spread': spread
                }

            records.append(record)

        df_records = pd.DataFrame(records)
        logging.info(f"Successfully created records dataframe for {book}")
        return df_records

    except KeyError as e:
        logging.error(f"Key error when calculating the spread: {e}")
        raise Exception(f"Key error when calculating the spread: {e}")
    
    except Exception as e:
        logging.error(f"Error when calculating spread: {e}")
        raise Exception(f"Error when calculating spread: {e}")

def save_records(df_records: pd.DataFrame, book: str, path='/usr/local/airflow/data'):
    
    # Define the sctructure to partition the data
    timestamp = datetime.now()
    year = timestamp.strftime("%Y")
    month = timestamp.strftime("%m")
    day = timestamp.strftime("%d")
    hour = timestamp.strftime("%H")

    # Define partitioned path
    partition_path = os.path.join(path, f"book={book}/year={year}/month={month}/day={day}/hour={hour}")
    os.makedirs(partition_path, exist_ok = True)

    # Save the data as parquet
    logging.info(f"Saving records to {partition_path}")
    parquet_table = pa.Table.from_pandas(df_records)
    pq.write_table(parquet_table, os.path.join(partition_path, f"order_book_data_{date_stamp}.parquet"))
    logging.info(f"Successfully saved records for {book}")

def etl_order_book(book: str, n_observations: int) -> None:
        
    try:
        logging.info(f"Starting ETL process for {book} with {n_observations} observations")
        observations = gather_observations(n_observations, book)
        df = create_records_dataframe(observations, book)

        save_records(df, book)
        logging.info(f"Completed ETL process for {book}")
    
    except Exception as e:
        logging.critical(f"ETL process failed for {book}: {e}")
        print(e)