import requests
import os
import time
import hmac
import hashlib
from dotenv import load_dotenv


# Load the API key and secret
load_dotenv()
API_KEY = os.getenv("API_KEY")
APY_SECRET = os.getenv("API_SECRET")

# Function to get the authorization header
def get_authorization_header()

# Define the URLS
url = 'https://sandbox.bitso.com/api/v3/open_orders?book=btc_mxn'

# Define the headers for the API call
headers = {
    key
}

requests.get(url)