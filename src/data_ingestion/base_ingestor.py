import sys
from config.settings import Settings
import os
import json
from utils.time import utc_now
import requests
from dotenv import load_dotenv
from pathlib import Path


class BaseIngestor:
    def __init__(self):
        env_path = Path(__file__).parents[2] / ".env"
        load_dotenv(dotenv_path=env_path)
        self.base_url = os.getenv("NFL_BASE_API_URL")
        self.api_key = os.getenv("NFL_API_KEY")
        # self.settings = Settings()
        self.headers = {
            "accept": "application/json",
            "x-api-key": self.api_key
        }

        # if self.settings.environment.upper() == "PROD":
        #     confirm = input(
        #         "⚠️ You are about to run against PROD."
        #         "Type 'yes' to continue: "
        #     )
        #     if confirm.lower() != "yes":
        #         print("❌ Aborted.")
        #         sys.exit(1)        I 
    
    def fetch_data(self,  url: str) -> dict:
        response = requests.get(url, headers=self.headers)
        response.raise_for_status()
        return response.json()
        
    def save_raw_json(self, data, folder_name):
        folder = os.path.join(".data", folder_name)
        os.makedirs(folder, exist_ok=True)
        timestamp = utc_now().strftime("%Y%m%d_%H%M%S")
        filename = os.path.join(folder, f"{folder_name}_{timestamp}.json")

        with open(filename, "w") as f:
            json.dump(data, f, indent=2)

        print(f"Saved raw data to {filename}")   

    def close(self):
        """Close the DB connection."""
        if self.conn:
            self.conn.close()