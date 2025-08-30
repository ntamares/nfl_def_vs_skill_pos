import os
from .base_ingestor import BaseIngestor
from datetime import datetime
from utils.db import safe_connection
from .player_depth_chart_ingestor import PlayerDepthChartIngestor
import time
import requests

class InjuriesIngestor(BaseIngestor):
    def __init__(self):
        super().__init__()
        self.endpoint_template = "seasons/{year}/REG/{week:02d}/injuries.json"
        
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
                        inj_week_db_id = cur.fetchone()[0]
                        
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
                            print(f"Team not found in DB: SR UUID={team['id']}")
                            continue

                    for player in team["players"]:
                        with conn.cursor() as cur:
                            player_db_id = None
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
                                
                                ingestor = PlayerDepthChartIngestor()
                                try:
                                    ingestor.insert_player(conn, player_row)
                                except Exception as e:
                                     print(f"Error inserting player {player["id"]}: {e}")
                        
                        if player_db_id is not None:
                            injuries = [
                                {
                                    "inj_player_id": player_db_id,
                                    "inj_team_id": team_db_id,
                                    "inj_season_year": year,
                                    "inj_week": i,
                                    "inj_status": injury.get("status", "Healthy"),
                                    "inj_status_date": datetime.fromisoformat(injury.get("status_date", "1970-01-01T00:00:00Z").replace("Z", "+00:00")),
                                    "inj_primary_injury": injury.get("primary",),
                                    "inj_week_id": inj_week_db_id,
                                   "inj_practice_participation": status_map[injury["practice"]["status"]]
                                }
                                for injury in player.get("injuries", [])
                                if "practice" in injury and "status" in injury["practice"]
                            ]
                        
                        for inj in injuries:
                            try:
                                self.insert_injury(conn, inj)
                            except Exception as e:
                                print(f"Error inserting injury for player {inj["inj_player_id"]}: {e}")
                conn.commit()
            
if __name__ == "__main__":
    ingestor = InjuriesIngestor()
    ingestor.run()