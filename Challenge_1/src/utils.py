import requests
import schedule
import os
import time
import hmac
import hashlib
import json
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from datetime import datetime
from dotenv import load_dotenv

# Load the API key and secret
load_dotenv(os.path.join(os.getcwd(), 'Challenge_1', 'src', '.env'))

API_KEY = os.getenv('API_KEY')
API_SECRET = os.getenv("API_SECRET")

def get_order_book(book: str) -> dict:
    base_url = "https://sandbox.bitso.com"
    request_path = f"/api/v3/order_book/?book={book}"

    # Make the GET request
    response = requests.get(f"{base_url}{request_path}")

    # Check the response
    if response.status_code == 200:
        return response.json()
    else:
        raise Exception(f"Failed to fetch data: {response.status_code} {response.text}")
    
# Define a function to gather the 600 observation from 10 minutes of orders
def gather_observations(seconds: int, book: str):

    observations = []

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
    
    return observations


def create_records_dataframe(observations: list, book: str):

    records = []

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

        return df_records

        #return best_ask, best_bid, spread

    except KeyError as e:
        raise Exception(f"Key error when calculating the spread: {e}")
    except Exception as e:
        raise Exception(f"Error when calculating spread: {e}")


def save_records(df_records: pd.DataFrame, path='data'): #os.path.join(os.getcwd(), 'Challenge_1', 'src', '.env')):
    
    # Define the sctructure to partition the data
    timestamp = datetime.now()
    timestamp_ff = timestamp.strftime("%Y%m%d%H%M")
    year = timestamp.strftime("%Y")
    month = timestamp.strftime("%m")
    day = timestamp.strftime("%d")
    hour = timestamp.strftime("%H")

    # Define partitioned path
    partition_path = os.path.join(path, f"year={year}/month={month}/day={day}/hour={hour}")
    os.makedirs(partition_path, exist_ok = True)

    # Save the data as parquet
    parquet_table = pa.Table.from_pandas(df_records)
    pq.write_table(parquet_table, os.path.join(partition_path, f"order_book_data{timestamp_ff}.parquet"))