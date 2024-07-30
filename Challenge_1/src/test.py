import requests
import os
import time
import hmac
import hashlib
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

def calculate_spread(order_book):
    try:
        bids = order_book['payload']['bids']
        asks = order_book['payload']['asks']

        # Find the best bid and best ask and convert to float
        best_bid = max(float(bid['price']) for bid in bids)
        best_ask = min(float(ask['price']) for ask in asks)

        # Calculate the spread
        spread = (best_ask - best_bid) * 100 / best_ask
        return best_ask, best_bid, spread

    except KeyError as e:
        raise Exception(f"Key error when calculating the spread: {e}")
    except Exception as e:
        raise Exception(f"Error when calculating spread: {e}")

try:
    order_book = get_order_book("btc_mxn")
    best_ask, best_bid, spread = calculate_spread(order_book)
except Exception as e:
    print(e)

print(f"best ask: {best_ask}")
print(f"best bid: {best_bid}")
print(f"spread: {spread}")