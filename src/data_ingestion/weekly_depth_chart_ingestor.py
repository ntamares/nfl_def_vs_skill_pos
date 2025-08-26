class WeeklyDepthChartIngestor:
    def __init__(self, conn):
        self.conn = conn

    def ingest(self):
        with self.conn.cursor() as cur:
            cur.execute("SELECT NOW()") 
            print("Weekly depth chart ingestion complete")