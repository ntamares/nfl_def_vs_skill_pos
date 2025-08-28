import os
import json
import requests
from pathlib import Path
from utils.db import safe_connection
from utils.time import utc_now
from dotenv import load_dotenv

class PlayerDepthChartIngestor():
    def __init__(self):
        env_path = Path(__file__).parents[2] / ".env"
        load_dotenv(dotenv_path=env_path)
        self.base_url = os.getenv("NFL_BASE_API_URL")
        self.api_key = os.getenv("NFL_API_KEY")
        self.endpoint_template = "seasons/{year}/REG/{week:02d}/depth_charts.json"
        self.headers = {
            "accept": "application/json",
            "x-api-key": self.api_key
        }

    def fetch_data(self,  url: str) -> dict:
        response = requests.get(url, headers=self.headers)
        response.raise_for_status()
        return response.json()
    
    def save_raw_json(self, data):
        folder = os.path.join("data", "depth_charts")
        os.makedirs(folder, exist_ok=True)
        timestamp = utc_now().strftime("%Y%m%d_%H%M%S")
        filename = os.path.join(folder, f"depth_charts_{timestamp}.json")

        with open(filename, "w") as f:
            json.dump(data, f, indent=2)

        print(f"Saved raw data to {filename}")    
    
    def player_exists(self, conn, player_id: int) -> bool:
        with conn.cursor() as cur:
            cur.execute("""
                select exists (
                    select 1 from refdata.player where player_sr_uuid = %s
                )
            """, (player_id,))
            return cur.fetchone()[0]

    def insert_player(self, conn, player_row):
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
            on conflict (player_sr_uuid) do nothing;
        """
        name_parts = player_row["name"].split(' ', 1)
        first_name = name_parts[0]
        last_name = name_parts[1] if len(name_parts) > 1 else ''
        
        with conn.cursor() as cur:
            cur.execute("""
                select team_id from refdata.team where team_sr_uuid = %s
            """, (player_row["team_id"],))
            team_row = cur.fetchone()
            db_team_id = team_row[0] if team_row else None
            cur.execute(
                query,
                (
                    player_row["name"],
                    first_name,
                    last_name,
                    db_team_id,
                    player_row["position"],
                    player_row["player_sr_uuid"],
                    player_row["jersey"],
                ),
            )
            
    def insert_depth_chart(self, conn, player_row):
        query = """
            insert into refdata.depth_chart_weekly
            (
                dc_team_id, 
                dc_season_year, 
                dc_week, 
                dc_player_id,
                dc_player_position, 
                dc_player_position_alignment,
                dc_rank
             )
             values(%s, %s, %s, %s, %s, %s, %s)
             on conflict (dc_team_id, dc_season_year, dc_week, dc_player_id, 
                dc_player_position, dc_player_position_alignment) do nothing
        """
        
        with conn.cursor() as cur:
            cur.execute("""
                select team_id from refdata.team where team_sr_uuid = %s
            """, (player_row["team_id"],))
            team_row = cur.fetchone()
            db_team_id = team_row[0] if team_row else None
            
            cur.execute("""
                select player_id from refdata.player where player_sr_uuid = %s
            """, (player_row["player_sr_uuid"],))
            player_id_row = cur.fetchone()
            db_player_id = player_id_row[0] if player_id_row else None
            
            cur.execute(
                query,
                (
                    db_team_id,
                    player_row["year"],
                    player_row["week"],
                    db_player_id,
                    player_row["position"],
                    player_row["position_alignment"],
                    player_row["rank"]
                )
            )

    def run(self):
        with safe_connection() as conn:
            year = 2024 
            for i in range(1, 19):
                endpoint = self.endpoint_template.format(year=year, week=i)
                url = f"{self.base_url}{endpoint}"
                data = self.fetch_data(url)
                
                if os.getenv("ENVIRONMENT", "DEV").upper() == "DEV":
                    self.save_raw_json(data)
                    
                players = [
                    {
                        "team_id": team["id"],
                        "player_sr_uuid": player["id"],
                        "name": player["name"],
                        "position": player["position"], # e.g., WR, RB, etc.
                        "position_alignment": pos["position"].get("name"),  # e.g., LWR, WR, RWR
                        "rank": player.get("depth"),
                        "jersey": player.get("jersey"),
                        "year": data["season"]["year"] if "season" in data 
                            and "year" in data["season"] else None,
                        "week": data["week"]["sequence"] if "week" in data 
                            and "sequence" in data["week"] else None
                    }
                    for team in data["teams"]
                    for group in ["offense", "defense", "special_teams"]
                    for pos in team.get(group, [])
                    if "position" in pos and "players" in pos["position"]
                    for player in pos["position"]["players"]
                ]
                
                print(f"Found {len(players)} players to process")
                
                for player_row in players:
                    print(f"Processing player: {player_row['name']} (sr_uuid: {player_row['player_sr_uuid']})")
                    exists = self.player_exists(conn, player_row["player_sr_uuid"])
                    print(f"Exists in DB: {exists}")
                    
                    if not exists:
                        try:
                            self.insert_player(conn, player_row)
                        except Exception as e:
                            print(f"Error inserting player {player_row['name']}: {e}")
                            
                    rank = player_row["rank"] if player_row["rank"] is not None else -1
                   
                    if rank == -1:
                        print(f"Warning: No rank for player {player_row['name']} ({player_row['player_sr_uuid']}) - using default -1")
                    
                    # maintain data integrity when we the rank is missing 
                    # and we need to set it to 0
                    # looking at you SR
                    player_row_copy = dict(player_row)
                    player_row_copy["rank"] = rank
                    
                    try:
                        self.insert_depth_chart(conn, player_row_copy)
                    except Exception as e:
                        print(f"Error inserting depth chart for player {player_row['name']}: {e}")
            conn.commit()

if __name__ == "__main__":
    ingestor = PlayerDepthChartIngestor()
    ingestor.run()