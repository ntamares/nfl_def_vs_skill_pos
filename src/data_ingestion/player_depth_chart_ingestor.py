import os
from utils.db import safe_connection
from .base_ingestor import BaseIngestor

class PlayerDepthChartIngestor(BaseIngestor):
    def __init__(self):
        super().__init__()
        self.endpoint_template = "seasons/{year}/REG/{week:02d}/depth_charts.json"

    def save_raw_json(self, data):
        super().save_raw_json(data, "depth_charts")
            
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
            team_map = self.get_team_map(conn)
            db_team_id = team_map.get(player_row["team_id"])
            
            db_player_id = self.get_player_id(conn, player_row["player_sr_uuid"])
            
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
                    
                # Extract player data with normalized rank
                players = [
                    {
                        "team_id": team["id"],
                        "player_sr_uuid": player["id"],
                        "name": player["name"],
                        "position": player["position"], # e.g., WR, RB, etc.
                        "position_alignment": pos["position"].get("name"),  # e.g., LWR, WR, RWR
                        "rank": player.get("depth") if player.get("depth") is not None else -1,
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
                
                # First, insert all players
                for player_row in players:
                    try:
                        self.insert_player(conn, player_row)
                    except Exception as e:
                        print(f"Error inserting player {player_row['name']}: {e}")
                
                # Then, insert all depth chart entries
                for player_row in players:
                    # Log warnings for players with default rank
                    if player_row["rank"] == -1:
                        print(f"Warning: No rank for player {player_row['name']} ({player_row['player_sr_uuid']}) - using default -1")
                    
                    try:
                        self.insert_depth_chart(conn, player_row)
                    except Exception as e:
                        print(f"Error inserting depth chart for player {player_row['name']}: {e}")
            conn.commit()

if __name__ == "__main__":
    ingestor = PlayerDepthChartIngestor()
    ingestor.run()