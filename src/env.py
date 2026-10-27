"""Explicit worker configuration; HTTP health does not load inference packages."""

from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(
        env_file=".env", extra="ignore", case_sensitive=False
    )
    port: int = 8080
    demucssvc_token: str = ""
    api_webhook_url_allowlist: list[str] = Field(default_factory=list)
    media_url_origins: list[str] = Field(default_factory=list)
    redis_url: str = "redis://localhost:6379/0"
    audio_redis_prefix: str = "track-tree-audio:"
    ffmpeg_path: str = "/usr/bin/ffmpeg"
    ffprobe_path: str = "/usr/bin/ffprobe"
    audio_device: str = "cpu"
    audio_max_input_bytes: int = 64 * 1024 * 1024
    audio_max_output_bytes: int = 256 * 1024 * 1024
    audio_job_timeout_seconds: int = 1500
    audio_worker_threads: int = 2
    audio_job_retention_seconds: int = 7 * 86400
    renditions_enabled: bool = False
    rendition_api_base_url: str = ""
    rendition_work_root: str = ""


settings = Settings()
