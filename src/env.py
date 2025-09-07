"""Environment configuration using pydantic-settings."""

import os
from typing import List, Optional
from pydantic import Field
from pydantic_settings import BaseSettings


class Settings(BaseSettings):
    """Application settings loaded from environment variables."""
    
    # Environment Configuration
    environment: str = Field(default="local", description="Environment: local, dev, staging, prod")
    
    # Server Configuration
    port: int = Field(default=8080, description="Server port")
    
    # Security
    demucssvc_token: str = Field(..., description="HMAC secret token for authentication")
    
    # Webhook Configuration
    api_webhook_url_allowlist: List[str] = Field(
        default_factory=lambda: ["https://api.track-tree.com/webhooks/demucs"],
        description="Allowed webhook URLs"
    )
    
    # S3/R2 Configuration
    s3_endpoint: Optional[str] = Field(default=None, description="S3 endpoint URL")
    s3_region: str = Field(default="us-east-1", description="S3 region")
    s3_bucket: str = Field(..., description="S3 bucket name")
    s3_access_key_id: Optional[str] = Field(default=None, description="S3 access key ID")
    s3_secret_access_key: Optional[str] = Field(default=None, description="S3 secret access key")
    
    # Redis Configuration
    redis_url: str = Field(default="redis://localhost:6379/0", description="Redis URL for Celery")
    
    # GPU Configuration
    cuda_visible_devices: str = Field(default="0", description="CUDA devices to use")
    
    @property
    def is_local(self) -> bool:
        """Check if running in local environment."""
        return self.environment.lower() == "local"
    
    @property
    def is_production(self) -> bool:
        """Check if running in production environment."""
        return self.environment.lower() == "prod"
    
    def get_s3_config(self) -> dict:
        """Get S3 configuration based on environment."""
        if self.is_local:
            # LocalStack configuration
            return {
                "endpoint_url": "http://localhost:4566",
                "region_name": "us-east-1",
                "aws_access_key_id": "test",
                "aws_secret_access_key": "test",
                "bucket": self.s3_bucket
            }
        else:
            # Production configuration
            return {
                "endpoint_url": self.s3_endpoint,
                "region_name": self.s3_region,
                "aws_access_key_id": self.s3_access_key_id,
                "aws_secret_access_key": self.s3_secret_access_key,
                "bucket": self.s3_bucket
            }
    
    class Config:
        env_file = ".env.local" if os.getenv("ENVIRONMENT") == "local" else ".env"
        env_file_encoding = "utf-8"
        case_sensitive = False


# Global settings instance
settings = Settings()
