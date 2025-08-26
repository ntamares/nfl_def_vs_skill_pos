import os
from pydantic_settings import BaseSettings, SettingsConfigDict

# Pick the right env file based on ENVIRONMENT variable
ENVIRONMENT = os.getenv("ENVIRONMENT", "DEV").upper()
env_file = os.path.join(os.path.dirname(__file__), ".env.dev" if ENVIRONMENT == "DEV" else ".env.prod")

class Settings(BaseSettings):
    DB_HOST: str
    DB_PORT: int
    DB_NAME: str
    DB_USER: str
    DB_PASSWORD: str
    ENVIRONMENT: str

    model_config = SettingsConfigDict(env_file=env_file, env_file_encoding="utf-8", extra="allow")

settings = Settings()