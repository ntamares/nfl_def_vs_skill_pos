import json
import os
import sys
from pathlib import Path
import requests
from dotenv import load_dotenv
from src.config.settings import Settings
from src.utils.time import utc_now

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

    def insert_player(self, conn, player_data):
        query = """
            insert into refdata.player 
            (
                player_name, 
                player_first_name, 
                player_last_name, 
                player_team_id, 
                player_position, 
                player_sr_uuid, 
                player_number
            )
            values (%s, %s, %s, %s, %s, %s, %s)
            on conflict (player_sr_uuid) do update set
                player_name = excluded.player_name,
                player_first_name = excluded.player_first_name,
                player_last_name = excluded.player_last_name,
                player_team_id = coalesce(excluded.player_team_id, refdata.player.player_team_id),
                player_position = coalesce(excluded.player_position, refdata.player.player_position),
                player_number = coalesce(excluded.player_number, refdata.player.player_number)
            returning player_id;
        """

        name_parts = player_data["name"].split(' ', 1)
        first_name = name_parts[0]
        last_name = name_parts[1] if len(name_parts) > 1 else ''
        
        team_id = None
        if player_data.get("team_id"):
            with conn.cursor() as cur:
                cur.execute("""
                    select team_id from refdata.team where team_sr_uuid = %s
                """, (player_data["team_id"],))
                team_row = cur.fetchone()
                team_id = team_row[0] if team_row else None
        
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
                select player_id
                from refdata.player
                where player_sr_uuid = %s
                """, (player_uuid,))
            player_row = cur.fetchone()
            return player_row[0] if player_row else None
    
    
    def get_team_map(self, conn):
        team_map = {}
        with conn.cursor() as cur:
            cur.execute("select team_id, team_sr_uuid from refdata.team")
            for row in cur.fetchall():
                team_map[row[1]] = row[0]
        return team_map
