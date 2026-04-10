import os
from dataclasses import dataclass

from dotenv import load_dotenv

load_dotenv()


@dataclass(frozen=True)
class Settings:
    DB_DRIVER: str = os.getenv("DB_DRIVER", "postgresql+psycopg2")
    DB_HOST: str = os.getenv("DB_HOST", "localhost")
    DB_PORT: str = os.getenv("DB_PORT", "55432")
    DB_NAME: str = os.getenv("DB_NAME", "project_analytics")
    DB_USER: str = os.getenv("DB_USER", "analytics_user")
    DB_PASSWORD: str = os.getenv("DB_PASSWORD", "analytics123")
    DATA_DIR: str = os.getenv("DATA_DIR", "./data")
    LOG_LEVEL: str = os.getenv("LOG_LEVEL", "INFO")

    @property
    def DATABASE_URL(self) -> str:
        return (
            f"{self.DB_DRIVER}://{self.DB_USER}:{self.DB_PASSWORD}"
            f"@{self.DB_HOST}:{self.DB_PORT}/{self.DB_NAME}"
        )


settings = Settings()
