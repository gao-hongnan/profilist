from __future__ import annotations

import shutil
import subprocess
from pathlib import Path

from ..models import ProfileResult, ScaleneConfig


class ScaleneProfiler:
    def __init__(self, config: ScaleneConfig | None = None) -> None:
        self.config = config or ScaleneConfig()

        if not shutil.which("scalene"):
            raise ImportError("Scalene not installed. Install with: pip install scalene")

    def profile_script(self, script_path: Path, *args: str) -> ProfileResult:
        output_path = self.config.output_path or Path(f"profile.{self.config.output_format}")
        output_path.parent.mkdir(parents=True, exist_ok=True)

        cmd = [
            "scalene",
            "--outfile",
            str(output_path),
            f"--{self.config.output_format}",
        ]

        if self.config.cpu_only:
            cmd.append("--cpu-only")
        if self.config.reduced_profile:
            cmd.append("--reduced-profile")

        cmd.extend([str(script_path), *args])

        result = subprocess.run(cmd, capture_output=True, text=True, check=False)

        if result.returncode != 0 and not output_path.exists():
            raise RuntimeError(f"Scalene profiling failed: {result.stderr}")

        return ProfileResult(
            report_path=output_path if output_path.exists() else None,
            raw_stats={"returncode": result.returncode, "stderr": result.stderr or ""},
        )
