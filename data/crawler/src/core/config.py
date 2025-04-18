import os
from pathlib import Path

from dotenv import load_dotenv
from pydantic import PostgresDsn, Field
from pydantic_settings import BaseSettings

BASE_DIR = Path(__file__).resolve().parent.parent.parent
env_file_path = os.path.join(BASE_DIR, ".env")

load_dotenv(env_file_path)

class DatabaseSettings(BaseSettings):
    host: str = "localhost"
    port: int = 5432
    user: str = "postgres"
    password: str = "postgres"
    name: str = "emogen"

    @property
    def uri(self) -> PostgresDsn:
        return f"postgresql+psycopg2://{self.user}:{self.password}@{self.host}:{self.port}/{self.name}"

    class Config:
        env_prefix = "DB_"


class KafkaSettings(BaseSettings):
    bootstrap_servers: str = "localhost:9092"
    emoji_topic: str = "emogen-crawler-topic"
    group_id: str = "emogen-crawler-group"

    class Config:
        env_prefix = "KAFKA_"


class MinioSettings(BaseSettings):
    endpoint: str = "localhost:9000"
    access_key: str = "minioadmin"
    secret_key: str = "minioadmin"
    bucket_name: str = "emojis"
    secure: bool = False

    class Config:
        env_prefix = "MINIO_"


class CrawlerSettings(BaseSettings):
    headless: bool = True
    page_limit: int = 10
    wait_time: int = 10
    sleep_time: int = 5

    class Config:
        env_prefix = "CRAWLER_"


class Settings(BaseSettings):
    db: DatabaseSettings = Field(default_factory=DatabaseSettings)
    kafka: KafkaSettings = Field(default_factory=KafkaSettings)
    minio: MinioSettings = Field(default_factory=MinioSettings)
    crawler: CrawlerSettings = Field(default_factory=CrawlerSettings)

    class Config:
        env_file = ".env"
        env_file_encoding = "utf-8"


config = Settings()
