from functools import lru_cache

from dotenv import load_dotenv
from pydantic import SecretStr
from pydantic_settings import BaseSettings

load_dotenv()


class Settings(BaseSettings):
    # General settings
    VERSION: str = "0.0.1"
    ENVIRONMENT: str = "dev"

    # Database settings (postgres)
    POSTGRES_HOST: str
    POSTGRES_PORT: int
    POSTGRES_DB: str
    POSTGRES_USER: str
    POSTGRES_PASSWORD: SecretStr

    # Kafka settings
    KAFKA_BOOTSTRAP_SERVERS: str
    KAFKA_SCHEMA_REGISTRY_URL: str
    KAFKA_CONSUMER_GROUP_ID: str

    # Dead-Letter Queue settings
    DLQ_TOPIC: str

    # Correction mechanism settings
    CORRECTION_INTERVAL: int = 300

    class Config:
        case_sensitive = False
        env_file_encoding = "utf-8"


settings = Settings()


@lru_cache()
def get_settings() -> BaseSettings:
    return Settings()
