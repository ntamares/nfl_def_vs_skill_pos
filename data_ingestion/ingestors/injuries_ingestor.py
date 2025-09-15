import os
import time
import datetime
import logging
import requests
from ..utils.db import safe_connection
from .base_ingestor import BaseIngestor

class InjuriesIngestor(BaseIngestor):
    def __init__(self):
        super().__init__()
        self.endpoint_template = "seasons/{year}/REG/{week:02d}/injuries.json"
        self.logger = logging.getLogger(__name__)
        
    def insert_injury(self, conn, inj):
        query = """
            insert into refdata.injury_weekly
            (
                inj_player_id, 
                inj_team_id, 
                inj_season_year, 
                inj_week_number,
                inj_status, 
                inj_status_date,
                inj_primary_injury,
                inj_week_id,
                inj_practice_participation
             )
             values(%s, %s, %s, %s, %s, %s, %s, %s, %s)
             on conflict (inj_player_id, inj_season_year, inj_week_number) do nothing
        """
        
        with conn.cursor() as cur:
            cur.execute(
                query,
                (
                    inj["inj_player_id"],
                    inj["inj_team_id"],
                    inj["inj_season_year"],
                    inj["inj_week"],
                    inj["inj_status"],
                    inj["inj_status_date"],
                    inj["inj_primary_injury"],
                    inj["inj_week_id"],
                    inj["inj_practice_participation"]
                )
            )
    
    
    def run(self):
        with safe_connection() as conn:
            year = 2024
            
            for i in range(1, 19):
                while True:
                    try:
                        url = f"{self.base_url}{self.endpoint_template.format(year=year, week = i)}"
                        data = self.fetch_data(url)
                        break
                    except requests.exceptions.HTTPError as e:
                        if e.response.status_code == 429:
                            print("Rate limit hit, sleeping...")
                            time.sleep(5)
                if os.getenv("ENVIRONMENT", "DEV").upper() == "DEV":
                        self.save_raw_json(data, "injuries")
                
                for team in data["teams"]:
                    with conn.cursor() as cur:
                        cur.execute("""
                                    select week_id from refdata.week 
                                    where week_sr_uuid = %s""", (data["week"].get("id"),))
                        week_result = cur.fetchone()

                        if week_result is not None:
                            inj_week_db_id = week_result[0]
                        else:
                            self.logger.error(f"Error: week not found in DB: SR UUID={data['week'].get('id')}")
                            continue
                        
                        status_map = {
                            "Did Not Participate In Practice": "DNP",
                            "Limited Participation In Practice": "Limited",
                            "Full Participation In Practice": "Full"
                        }
                        
                        team_db_id = None
                        cur.execute("""
                            select team_id from refdata.team 
                            where team_sr_uuid = %s""", (team.get("id"),))
                        result = cur.fetchone()
                        if result is not None:
                            team_db_id = result[0]
                        else:
                            self.logger.error(f"Error: team not found in DB: SR UUID={team['id']}")
                            continue
                    
                    player_db_id = None

                    for player in team["players"]:
                        with conn.cursor() as cur:
                            cur.execute("""
                                select player_id from refdata.player 
                                where player_sr_uuid = %s""", (player.get("id"),))
                            result = cur.fetchone()
                            
                            if result is not None:
                                player_db_id = result[0]
                            else:
                                player_row = {
                                    "name": player["name"],
                                    "position": player["position"],
                                    "player_sr_uuid": player["id"],
                                    "jersey": player["jersey"],
                                    "team_id": team.get("id")
                                }
                                
                                try:
                                    player_db_id = self.insert_player(conn, player_row)
                                    self.logger.info(f"Successfully inserted player {player['name']} with ID {player_db_id}")
                                except Exception as e:
                                     self.logger.error(f"Error inserting player {player['id']}: {e}")
                        
                        if player_db_id is not None:
                            injuries = [
                                {
                                    "inj_player_id": player_db_id,
                                    "inj_team_id": team_db_id,
                                    "inj_season_year": year,
                                    "inj_week": i,
                                    "inj_status": injury.get("status", "Healthy"),
                                    "inj_status_date": datetime.datetime.fromisoformat(injury.get("status_date", "1970-01-01T00:00:00Z").replace("Z", "+00:00")),
                                    "inj_primary_injury": injury.get("primary"),
                                    "inj_week_id": inj_week_db_id,
                                    "inj_practice_participation": status_map.get(injury["practice"]["status"], "Unknown")
                                }
                                for injury in player.get("injuries", [])
                                if "practice" in injury and "status" in injury["practice"] and injury["practice"]["status"] in status_map
                            ]
                            
                            for inj in injuries:
                                try:
                                    self.insert_injury(conn, inj)
                                    self.logger.info(f"Successfully inserted injury for player {player['name']} (ID: {player_db_id})")
                                except Exception as e:
                                    self.logger.error(f"Error inserting injury for player {player['name']} (ID: {player_db_id}): {e}")
                conn.commit()
            
if __name__ == "__main__":
    logs_dir = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), '.logs')
    os.makedirs(logs_dir, exist_ok=True)
    timestamp = datetime.datetime.now().strftime('%Y%m%d_%H%M%S')
    log_filename = os.path.join(logs_dir, f'injuries_ingestor_{timestamp}.log')
    
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        handlers=[
            logging.FileHandler(log_filename),
            logging.StreamHandler() 
        ]
    )
    
    logging.info(f"Logging to file: {log_filename}")
    ingestor = InjuriesIngestor()
    ingestor.run()
    
    logging.info("Depth chart script execution completed")
    print(f"\nScript execution completed. Full logs saved to: {log_filename}")