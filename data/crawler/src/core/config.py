import os
import json
from pathlib import Path
from typing import Any, Dict

from dotenv import load_dotenv
from pydantic import PostgresDsn, Field, field_validator
from pydantic_settings import BaseSettings, SettingsConfigDict

BASE_DIR = Path(__file__).resolve().parent.parent.parent
env_file_path = os.path.join(BASE_DIR, ".env")

load_dotenv(env_file_path)


class DatabaseSettings(BaseSettings):
    host: str = Field(default="localhost")
    port: int = Field(default=5432)
    user: str = Field(default="postgres")
    password: str = Field(default="postgres")
    name: str = Field(default="emogen")

    model_config = SettingsConfigDict(
        env_prefix="DB_",
        case_sensitive=False,
        extra="ignore"
    )

    @property
    def uri(self) -> PostgresDsn:
        return f"postgresql+psycopg2://{self.user}:{self.password}@{self.host}:{self.port}/{self.name}"


class KafkaSettings(BaseSettings):
    bootstrap_servers: str = Field(default="localhost:9092")
    emoji_topic: str = Field(default="emogen-crawler-topic")
    group_id: str = Field(default="emogen-crawler-group")

    model_config = SettingsConfigDict(
        env_prefix="KAFKA_",
        case_sensitive=False,
        extra="ignore"
    )


class MinioSettings(BaseSettings):
    endpoint: str = Field(default="localhost:9000")
    access_key: str = Field(default="minioadmin")
    secret_key: str = Field(default="minioadmin")
    bucket_name: str = Field(default="emojis")
    secure: bool = Field(default=False)

    model_config = SettingsConfigDict(
        env_prefix="MINIO_",
        case_sensitive=False,
        extra="ignore"
    )


class CrawlerSettings(BaseSettings):
    headless: bool = Field(default=True)
    page_limit: int = Field(default=10)
    wait_time: int = Field(default=10)
    sleep_time: int = Field(default=5)

    model_config = SettingsConfigDict(
        env_prefix="CRAWLER_",
        case_sensitive=False,
        extra="ignore"
    )


class SparkSettings(BaseSettings):
    app_name: str = Field(default="emogen-crawler")
    master: str = Field(default="local[*]")
    configs: Dict[str, Any] = Field(default_factory=dict)

    model_config = SettingsConfigDict(
        env_prefix="SPARK_",
        case_sensitive=False,
        extra="ignore"
    )

    @field_validator("configs", mode="before")
    @classmethod
    def parse_configs(cls, v):
        """Parse configs from string to dict, handling errors gracefully"""
        if isinstance(v, dict):
            return v
        if not v:
            return {}
        try:
            return json.loads(v)
        except (json.JSONDecodeError, TypeError):
            print(
                f"Warning: Could not parse SPARK_CONFIGS as JSON. Using empty dict instead. Value: {v}")
            return {}


class Settings(BaseSettings):
    db: DatabaseSettings = Field(default_factory=DatabaseSettings)
    kafka: KafkaSettings = Field(default_factory=KafkaSettings)
    minio: MinioSettings = Field(default_factory=MinioSettings)
    crawler: CrawlerSettings = Field(default_factory=CrawlerSettings)
    spark: SparkSettings = Field(default_factory=SparkSettings)

    model_config = SettingsConfigDict(
        env_file=env_file_path,
        env_file_encoding="utf-8",
        extra="ignore"
    )


config = Settings()
