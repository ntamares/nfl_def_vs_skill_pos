import sys
from config.settings import Settings
import os
import json
import time
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
        self.settings = Settings()
        self.headers = {
            "accept": "application/json",
            "x-api-key": self.api_key
        }

        if os.getenv("ENVIRONMENT", "PROD").upper() == "PROD":
            confirm = input(
                "You are about to run against PROD."
                "Type 'yes' to continue: "
            )
            if confirm.lower() != "yes":
                print("Aborted.")
                sys.exit(1)     
    
    def fetch_data(self, url: str) -> dict:
        """
        Fetch data from the API
        
        Args:
            url: The URL to fetch data from
            
        Returns:
            The JSON response data
        """
        response = requests.get(url, headers=self.headers)
        response.raise_for_status()  # Will raise HTTPError for 4XX/5XX responses
        return response.json()
        
    def save_raw_json(self, data, folder_name):
        folder = os.path.join(".data", folder_name)
        os.makedirs(folder, exist_ok=True)
        timestamp = utc_now().strftime("%Y%m%d_%H%M%S")
        filename = os.path.join(folder, f"{folder_name}_{timestamp}.json")

        with open(filename, "w") as f:
            json.dump(data, f, indent=2)

        print(f"Saved raw data to {filename}")   

    def insert_player(self, conn, player_data):
        query = """
            INSERT INTO refdata.player 
            (
                player_name, 
                player_first_name, 
                player_last_name, 
                player_team_id, 
                player_position, 
                player_sr_uuid, 
                player_number
            )
            VALUES (%s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (player_sr_uuid) DO UPDATE SET
                player_name = EXCLUDED.player_name,
                player_first_name = EXCLUDED.player_first_name,
                player_last_name = EXCLUDED.player_last_name,
                player_team_id = COALESCE(EXCLUDED.player_team_id, refdata.player.player_team_id),
                player_position = COALESCE(EXCLUDED.player_position, refdata.player.player_position),
                player_number = COALESCE(EXCLUDED.player_number, refdata.player.player_number)
            RETURNING player_id;
        """

        name_parts = player_data["name"].split(' ', 1)
        first_name = name_parts[0]
        last_name = name_parts[1] if len(name_parts) > 1 else ''
        
        team_id = None
        if player_data.get("team_id"):
            with conn.cursor() as cur:
                cur.execute("""
                    SELECT team_id FROM refdata.team WHERE team_sr_uuid = %s
                """, (player_data["team_id"],))
                team_row = cur.fetchone()
                team_id = team_row[0] if team_row else None
        
        # Execute the insert/update
        with conn.cursor() as cur:
            cur.execute(
                query,
                (
                    player_data["name"],
                    first_name,
                    last_name,
                    team_id,
                    player_data.get("position"),
                    player_data["player_sr_uuid"],
                    player_data.get("jersey")
                ),
            )
            result = cur.fetchone()
            return result[0] if result else None
            
    def get_player_id(self, conn, player_uuid):
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT player_id
                FROM refdata.player
                WHERE player_sr_uuid = %s
                """, (player_uuid,))
            player_row = cur.fetchone()
            return player_row[0] if player_row else None
    
    def get_team_map(self, conn):
        team_map = {}
        with conn.cursor() as cur:
            cur.execute("SELECT team_id, team_sr_uuid FROM refdata.team")
            for row in cur.fetchall():
                team_map[row[1]] = row[0]
        return team_map
