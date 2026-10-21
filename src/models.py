"""Contract 0.2 camelCase transport. No permissive legacy aliases."""

from typing import Literal
from uuid import UUID
from pydantic import BaseModel, ConfigDict, Field, model_validator

StemType = Literal["vocals", "drums", "bass", "other", "guitar", "piano", "custom"]


class WireModel(BaseModel):
    model_config = ConfigDict(extra="forbid")


class OutputGrant(WireModel):
    type: StemType
    fileKey: str = Field(min_length=1)
    uploadUrl: str


class SplitRequest(WireModel):
    jobId: UUID
    versionId: UUID
    audioUrl: str
    model: str = Field(min_length=1, max_length=100)
    callbackUrl: str
    outputs: list[OutputGrant] = Field(min_length=1, max_length=8)
    correlationId: str | None = None


class SplitResponse(WireModel):
    jobId: UUID
    status: Literal["accepted"] = "accepted"


class StemInfo(WireModel):
    type: StemType
    fileKey: str
    sizeBytes: int = Field(ge=0)
    durationMs: int = Field(ge=0)
    checksumSha256: str = Field(pattern=r"^[0-9a-f]{64}$")


class WebhookPayload(WireModel):
    jobId: UUID
    versionId: UUID
    status: Literal["completed", "failed"]
    stems: list[StemInfo]
    processingTimeMs: int = Field(ge=0)
    error: str | None = None

    @model_validator(mode="after")
    def validate_terminal(self):
        if self.status == "completed" and (not self.stems or self.error is not None):
            raise ValueError("Completed callback requires stems and no error")
        if self.status == "failed" and (self.stems or not self.error):
            raise ValueError("Failed callback requires an error and no stems")
        return self


class HealthResponse(WireModel):
    status: Literal["ok"] = "ok"
