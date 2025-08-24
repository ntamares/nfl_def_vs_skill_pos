import requests
import os
import psycopg 
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

# Get the API key from the environment
api_key = os.getenv("API_KEY")

url = "https://api.sportradar.com/nfl/official/trial/v7/en/league/teams.json"

headers = {
    "accept": "application/json",
    "x-api-key": api_key
}

response = requests.get(url, headers=headers)

print(response.text)