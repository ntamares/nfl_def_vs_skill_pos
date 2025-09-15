import os
from pydantic_settings import BaseSettings, SettingsConfigDict

ENVIRONMENT = os.getenv("ENVIRONMENT", "DEV").upper()
root_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), "../.."))
env_file = os.path.join(root_dir, ".env.prod" if ENVIRONMENT == "PROD" else ".env.dev")

class Settings(BaseSettings):
    DB_HOST = os.environ.get("DB_HOST")
    DB_PORT = int(os.environ.get("DB_PORT", 5432))
    DB_NAME = os.environ.get("DB_NAME")
    DB_USER = os.environ.get("DB_USER")
    DB_PASSWORD = os.environ.get("DB_PASSWORD")
    ENVIRONMENT = os.environ.get("ENVIRONMENT", "dev")

    model_config = SettingsConfigDict(env_file=env_file, env_file_encoding="utf-8", extra="allow")

settings = Settings()