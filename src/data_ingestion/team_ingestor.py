import os
import json
import requests
from pathlib import Path
from utils.db import safe_connection
from utils.time import utc_now
from dotenv import load_dotenv

class TeamIngestor():
    def __init__(self):
        env_path = Path(__file__).parents[2] / ".env"
        load_dotenv(dotenv_path=env_path)
        self.base_url = os.getenv("NFL_BASE_API_URL")
        self.api_key = os.getenv("NFL_API_KEY")
        self.endpoint = "/teams.json"
        self.headers = {
            "accept": "application/json",
            "x-api-key": self.api_key
        }

    def fetch_data(self):
        url = f"{self.base_url}{self.endpoint}"
        response = requests.get(url, headers=self.headers)
        response.raise_for_status()
        return response.json()

    def save_raw_json(self, data):
        folder = os.path.join("data", "teams")
        os.makedirs(folder, exist_ok=True)

        timestamp = utc_now().strftime("%Y%m%d_%H%M%S")
        filename = os.path.join(folder, f"teams_{timestamp}.json")

        with open(filename, "w") as f:
            json.dump(data, f, indent=2)

        print(f"Saved raw data to {filename}")

    def insert_data(self, data):
        query = """
            INSERT INTO refdata.team 
            (team_sr_uuid, team_name, team_market, team_abbreviation)
            VALUES (%s, %s, %s, %s)
            ON CONFLICT (team_sr_uuid) DO NOTHING;
        """

        with safe_connection() as conn:
            with conn.cursor() as cur:
                for team in data.get("teams", []):
                    if (team["name"] != "TBD"):
                        cur.execute(
                            query,
                            (
                                team["id"],
                                team["name"],
                                team["market"],
                                team["alias"],
                            ),
                        )
            conn.commit()
            print(f"✅ Inserted {len(data.get('teams', []))} teams")

    def run(self):
        data = self.fetch_data()
        # Only save raw JSON if running in DEV environment
        if os.getenv("ENVIRONMENT", "DEV").upper() == "DEV":
            self.save_raw_json(data)
        self.insert_data(data)


if __name__ == "__main__":
    ingestor = TeamIngestor()
    ingestor.run()