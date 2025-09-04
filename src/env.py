"""Environment configuration using pydantic-settings."""

from typing import List
from pydantic import Field
from pydantic_settings import BaseSettings


class Settings(BaseSettings):
    """Application settings loaded from environment variables."""
    
    # Server Configuration
    port: int = Field(default=8080, description="Server port")
    
    # Security
    demucssvc_token: str = Field(..., description="HMAC secret token for authentication")
    
    # Webhook Configuration
    api_webhook_url_allowlist: List[str] = Field(
        default_factory=lambda: ["https://api.track-tree.com/webhooks/demucs"],
        description="Allowed webhook URLs"
    )
    
    # S3/R2 Configuration (optional)
    s3_endpoint: str = Field(default="", description="S3 endpoint URL")
    s3_region: str = Field(default="us-east-1", description="S3 region")
    s3_bucket: str = Field(default="", description="S3 bucket name")
    s3_access_key_id: str = Field(default="", description="S3 access key ID")
    s3_secret_access_key: str = Field(default="", description="S3 secret access key")
    
    # Redis Configuration
    redis_url: str = Field(default="redis://localhost:6379/0", description="Redis URL for Celery")
    
    # GPU Configuration
    cuda_visible_devices: str = Field(default="0", description="CUDA devices to use")
    
    class Config:
        env_file = ".env"
        env_file_encoding = "utf-8"
        case_sensitive = False


# Global settings instance
settings = Settings()
