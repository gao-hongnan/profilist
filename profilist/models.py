from __future__ import annotations

from pathlib import Path
from typing import Literal

from pydantic import BaseModel, ConfigDict, Field

SortBy = Literal["cumulative", "time", "calls", "name"]
OutputFormat = Literal["text", "html", "json"]


class ProfilerConfig(BaseModel):
    model_config = ConfigDict(arbitrary_types_allowed=True)

    name: str = Field(default="profile")
    output_path: Path | None = Field(default=None)
    silent: bool = Field(default=False)


class CProfileConfig(ProfilerConfig):
    sort_by: SortBy = Field(default="cumulative")
    top_n: int = Field(default=20, ge=1)


class ScaleneConfig(ProfilerConfig):
    output_format: OutputFormat = Field(default="html")
    cpu_only: bool = Field(default=False)
    reduced_profile: bool = Field(default=False)


class ProfileResult(BaseModel):
    model_config = ConfigDict(arbitrary_types_allowed=True)

    cpu_time: float | None = Field(default=None)
    memory_peak_mb: float | None = Field(default=None)
    memory_allocated_mb: float | None = Field(default=None)
    report_path: Path | None = Field(default=None)
    raw_stats: dict[str, float | str | None] = Field(default_factory=dict)

    @property
    def summary(self) -> str:
        parts = []
        if self.cpu_time is not None:
            parts.append(f"CPU: {self.cpu_time:.4f}s")
        if self.memory_peak_mb is not None:
            parts.append(f"Peak: {self.memory_peak_mb:.2f}MB")
        if self.memory_allocated_mb is not None:
            parts.append(f"Allocated: {self.memory_allocated_mb:.2f}MB")
        if self.report_path:
            parts.append(f"Report: {self.report_path}")
        return " | ".join(parts) if parts else "No profiling results"
