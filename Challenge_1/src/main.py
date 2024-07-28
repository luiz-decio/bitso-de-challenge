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

# Function to get the authorization header
def get_authorization_headers(method, request_path, body=""):
    nounce = str(int(time.time() * 1000)) # Use miliseconds for uniqueness
    print(f"nounce is: {nounce}")
    data = f"{nounce}{method}{request_path}{body}"
    print(f"data is: {data}")

    # Generate sha26 signature
    signature = hmac.new(
        API_SECRET.encode("utf-8"),
        data.encode("utf-8"),
        hashlib.sha256
    )

    # Build authorization header
    auth_header = f"Bitso {API_KEY}:{nounce}:{signature}"
    return {"Authorization": auth_header}

def get_order_book(book):
    base_url = "https://sandbox.bitso.com"
    request_path = f"/api/v3/order_book/?book={book}"
    #headers = get_authorization_headers("GET", request_path)

    # Make the GET request
    response = requests.get(f"{base_url}{request_path}")#, header=headers)
    print(f'API response: {response.status_code}')

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