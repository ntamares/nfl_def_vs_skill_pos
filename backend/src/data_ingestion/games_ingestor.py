import os
import logging
from datetime import datetime
from utils.db import safe_connection
from .base_ingestor import BaseIngestor

class GamesIngestor(BaseIngestor):
    def __init__(self):
        super().__init__()
        self.endpoint_template = "games/{year}/REG/schedule.json"
        self.logger = logging.getLogger(__name__)
        
    def insert_week(self, conn, week_row):
        query = """
            insert into refdata.week
            (
                week_sr_uuid, 
                week_season_year, 
                week_season_type, 
                week_number,
                week_start_date, 
                week_end_date
             )
             values(%s, %s, %s, %s, %s, %s)
             on conflict (week_season_year, week_season_type, week_number) do nothing
        """
        
        with conn.cursor() as cur:
            cur.execute(
                query,
                (
                    week_row["week_sr_uuid"],
                    week_row["week_season_year"],
                    week_row["week_season_type"],
                    week_row["week_number"],
                    week_row["week_start_date"],
                    week_row["week_end_date"]
                )
            )
    
    
    def insert_games(self, conn, game_row):
        query = """
            insert into refdata.game
            (
                game_week, 
                game_season_year, 
                game_home_team_id, 
                game_away_team_id,
                game_date, 
                game_home_score,
                game_away_score,
                game_sr_uuid,
                game_week_id
             )
             values(%s, %s, %s, %s, %s, %s, %s, %s, %s)
             on conflict (game_week, game_season_year, game_home_team_id, game_away_team_id) do nothing
        """
        with conn.cursor() as cur:
            cur.execute(
                query,
                (
                    game_row["game_week"],
                    game_row["game_season_year"],
                    game_row["game_home_team_id"],
                    game_row["game_away_team_id"],
                    game_row["game_date"],
                    game_row["game_home_score"],
                    game_row["game_away_score"],
                    game_row["game_sr_uuid"],
                    game_row["game_week_id"]
                )
            )
    
    
    def run(self):
        with safe_connection() as conn:
            year = 2024
            url = f"{self.base_url}/{self.endpoint_template.format(year=year)}"
            data = self.fetch_data(url)
            
            if os.getenv("ENVIRONMENT", "DEV").upper() == "DEV":
                self.save_raw_json(data, "games")

            for week in data["weeks"]:
                week_number = week["sequence"]
                
                game_dates = [
                    datetime.fromisoformat(g["scheduled"].replace("Z", "+00:00"))
                    for g in week["games"]
                    if "scheduled" in g
                ]
                
                if not game_dates:
                    self.logger.warning(f"Warning: No valid game dates found for week {week_number}")
                    continue
        
                week_row = {
                    "week_sr_uuid": week["id"],
                    "week_season_year": year,
                    "week_season_type": data["type"],
                    "week_number": week_number,
                    "week_start_date": min(game_dates),
                    "week_end_date": max(game_dates)
                }
                
                try:
                    self.insert_week(conn, week_row)
                    self.logger.info(f"Successfully inserted week {week_row['week_sr_uuid']}")
                except Exception as e:
                    self.logger.error(f"Error inserting week {week_row['week_sr_uuid']}: {e}")
                    continue
                        
                with conn.cursor() as cur:
                    cur.execute("""
                                select week_id from refdata.week 
                                where week_sr_uuid = %s""", (week["id"],))
                    result = cur.fetchone()
                    if not result:
                        self.logger.warning(f"Warning: Could not find week ID for week {week_number}")
                        continue
                    week_db_id = result[0]
                
                team_map = {}
                with conn.cursor() as cur:
                    cur.execute("select team_sr_uuid, team_id from refdata.team")
                    for row in cur.fetchall():
                        team_map[row[0]] = row[1]
                
                games_to_insert = []
                for game in week["games"]:
                    home_team_id = team_map.get(game["home"].get("id"))
                    away_team_id = team_map.get(game["away"].get("id"))
                    
                    if not home_team_id or not away_team_id:
                        self.logger.warning(f"Warning: Missing team ID for game {game.get('id')}")
                        continue
                    
                    try:
                        game_date = datetime.fromisoformat(game["scheduled"].replace("Z", "+00:00"))
                    except (KeyError, ValueError) as e:
                        self.logger.error(f"Error parsing game date for game {game.get('id')}: {e}")
                        continue
                    
                    game_row = {
                        "game_week": week_number,
                        "game_season_year": year,
                        "game_home_team_id": home_team_id,
                        "game_away_team_id": away_team_id,
                        "game_date": game_date,
                        "game_home_score": game.get("scoring", {}).get("home_points", 0),
                        "game_away_score": game.get("scoring", {}).get("away_points", 0),
                        "game_sr_uuid": game["id"],
                        "game_week_id": week_db_id
                    }
                    
                    games_to_insert.append(game_row)
                
                for game_row in games_to_insert:
                    try:
                        self.insert_games(conn, game_row)
                        self.logger.info(f"Successfully inserted game {game_row['game_sr_uuid']}")
                    except Exception as e:
                        self.logger.error(f"Error inserting game {game_row['game_sr_uuid']}: {e}")
                
            conn.commit()
            
if __name__ == "__main__":
    logs_dir = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), '.logs')
    os.makedirs(logs_dir, exist_ok=True)
    timestamp = datetime.datetime.now().strftime('%Y%m%d_%H%M%S')
    log_filename = os.path.join(logs_dir, f'games_ingestor_{timestamp}.log')
    
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        handlers=[
            logging.FileHandler(log_filename),
            logging.StreamHandler() 
        ]
    )
    
    logging.info(f"Logging to file: {log_filename}")
    ingestor = GamesIngestor()
    ingestor.run()
    logging.info("Games script execution completed")
    print(f"\nScript execution completed. Full logs saved to: {log_filename}")