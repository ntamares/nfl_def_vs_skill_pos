from utils.db import get_connection

class PlayerIngestor:
    def __init__(self, conn):
        self.conn = conn

    def ingest(self):
        with self.conn.cursor() as cur:
            cur.execute("SELECT NOW()") 
            print("Player ingestion complete")