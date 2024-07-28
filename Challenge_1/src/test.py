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

try:
    order_book = get_order_book("btc_mxn")
    print(order_book)
except Exception as e:
    print(e)