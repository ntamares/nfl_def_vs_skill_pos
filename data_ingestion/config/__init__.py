from dotenv import load_dotenv
import os

# Choose env
env = os.getenv("APP_ENV", "dev")  # default to dev
load_dotenv(f"config/{env}.env")

DATABASE_URL = os.getenv("DATABASE_URL")
DEBUG = os.getenv("DEBUG", "False").lower() == "true"