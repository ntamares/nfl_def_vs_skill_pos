import os
from utils.db import safe_connection
from utils.time import utc_now
from .base_ingestor import BaseIngestor

class TeamIngestor(BaseIngestor):
    def __init__(self):
        super().__init__()
        self.endpoint = "league/teams.json"

    def save_raw_json(self, data):
        super().save_raw_json(data, "teams")

    def insert_team(self, data):
        query = """
            insert into refdata.team 
            (team_sr_uuid, team_name, team_market, team_abbreviation)
            values (%s, %s, %s, %s)
            on conflict (team_sr_uuid) do nothing;
        """

        with safe_connection() as conn:
            valid_teams = [
                team for team in data.get("teams", [])
                if team.get("name") != "TBD"
            ]
            
            inserted_count = 0
            with conn.cursor() as cur:
                for team in valid_teams:
                    try:
                        cur.execute(
                            query,
                            (
                                team["id"],
                                team["name"],
                                team["market"],
                                team["alias"],
                            ),
                        )
                        inserted_count += 1
                    except Exception as e:
                        print(f"Error inserting team {team.get('name', 'unknown')}: {e}")
            
            conn.commit()
            print(f"✅ Inserted {inserted_count} teams out of {len(valid_teams)} valid teams")

    def run(self):
        url = f"{self.base_url}{self.endpoint}"
        data = self.fetch_data(url)
        
        if os.getenv("ENVIRONMENT", "DEV").upper() == "DEV":
            self.save_raw_json(data)
        
        self.insert_team(data)


if __name__ == "__main__":
    ingestor = TeamIngestor()
    ingestor.run()