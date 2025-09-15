import os
import datetime
import logging
from src.utils.db import safe_connection
from .base_ingestor import BaseIngestor

class DepthChartIngestor(BaseIngestor):
    def __init__(self):
        super().__init__()
        self.endpoint_template = "seasons/{year}/REG/{week:02d}/depth_charts.json"
        self.logger = logging.getLogger(__name__)

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
                    self.save_raw_json(data, "depth_charts")
                    
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
                
                self.logger.info(f"Found {len(players)} players to process")
                
                for player_row in players:
                    try:
                        self.insert_player(conn, player_row)
                        self.logger.info(f"Successfully inserted player {player_row['name']} - {player_row['player_sr_uuid']}")
                    except Exception as e:
                        self.logger.error(f"Error inserting player {player_row['name']} - {player_row['player_sr_uuid']} {e}")
                
                for player_row in players:
                    if player_row["rank"] == -1:
                        self.logger.warning(f"Warning: No rank for player {player_row['name']} ({player_row['player_sr_uuid']}) - using default -1")
                    
                    try:
                        self.insert_depth_chart(conn, player_row)
                        self.logger.info(f"Successfully inserted {player_row['name']} into refdata.depth_chart_weekly")
                    except Exception as e:
                        self.logger.error(f"Error inserting depth chart for player {player_row['name']}: {e}")
            conn.commit()
            self.logger.info(f"Successfully finished depth chart ingestion")

if __name__ == "__main__":
    logs_dir = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), '.logs')
    os.makedirs(logs_dir, exist_ok=True)
    timestamp = datetime.datetime.now().strftime('%Y%m%d_%H%M%S')
    log_filename = os.path.join(logs_dir, f'depth_chart_ingestor_{timestamp}.log')
    
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        handlers=[
            logging.FileHandler(log_filename),
            logging.StreamHandler() 
        ]
    )
    
    logging.info(f"Logging to file: {log_filename}")
    
    ingestor = DepthChartIngestor()
    ingestor.run()
    
    logging.info("Depth chart script execution completed")
    print(f"\nScript execution completed. Full logs saved to: {log_filename}")