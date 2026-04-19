from __future__ import annotations

from enum import Enum
from typing import Any

from pydantic import BaseModel, ConfigDict, Field, field_validator


class BatchType(str, Enum):
    ORIGINAL = "original"
    RERUN = "rerun"
    BACKFILL = "backfill"
    MERGED = "merged"
    MANUAL = "manual"


class GitProvenance(BaseModel):
    model_config = ConfigDict(extra="forbid")
    commit: str | None = None
    dirty: bool
    branch: str | None = None


class BatchMeta(BaseModel):
    model_config = ConfigDict(extra="forbid")
    experiment_id: str
    batch_id: str
    batch_type: BatchType
    created_at: str
    models: list[str] = Field(min_length=1)
    instances: dict[str, list[str]]
    git: GitProvenance
    provenance: dict[str, Any]

    @field_validator("experiment_id", "batch_id")
    @classmethod
    def validate_eid(cls, value: str) -> str:
        if not value.startswith("E") or not value[1:].isdigit():
            raise ValueError("expected E##### identifier")
        return value
