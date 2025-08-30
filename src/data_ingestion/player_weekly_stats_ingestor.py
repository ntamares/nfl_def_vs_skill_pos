import os
import json
from utils.db import safe_connection
from utils.time import utc_now
from .base_ingestor import BaseIngestor

class PlayerWeeklyStatsIngestor(BaseIngestor):
    def __init__(self):
        super().__init__()
        self.endpoint_template = "games/{game_id}/boxscore.json"
        
    def run(self):
        url = f"{self.base_url}/{self.endpoint_template.format(game_id=0)}"
        data = self.fetch_data(url)
        self.save_raw_json(data, "games")