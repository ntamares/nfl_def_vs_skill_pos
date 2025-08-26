import sys
from config.settings import get_settings
from utils.db import get_connection

class BaseIngestor:
    def __init__(self):
        self.settings = get_settings()

        if self.settings.environment.upper() == "PROD":
            confirm = input(
                "⚠️ You are about to run against PROD."
                "Type 'yes' to continue: "
            )
            if confirm.lower() != "yes":
                print("❌ Aborted.")
                sys.exit(1)

        self.conn = get_connection(self.settings)

    def close(self):
        """Close the DB connection."""
        if self.conn:
            self.conn.close()