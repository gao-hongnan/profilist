from __future__ import annotations

import cProfile
import io
import json
import pstats
from typing import TYPE_CHECKING, Any, Protocol, Self, cast

from ..models import CProfileConfig, ProfileResult

if TYPE_CHECKING:
    from typing import TextIO


class StatsWithStream(Protocol):
    stream: TextIO
    stats: dict[tuple[str, int, str], tuple[int, int, float, float, dict[Any, Any]]]
    total_tt: float

    def print_stats(self, *args: Any) -> None: ...
    def sort_stats(self, *args: Any) -> None: ...


class CProfileProfiler:
    def __init__(self, config: CProfileConfig | None = None) -> None:
        self.config = config or CProfileConfig()
        self._profiler = cProfile.Profile()
        self._stats: StatsWithStream | None = None

    def __enter__(self) -> Self:
        self._profiler.enable()
        return self

    def __exit__(self, *args: object) -> None:
        self._profiler.disable()
        stats = pstats.Stats(self._profiler)
        stats.sort_stats(self.config.sort_by)
        self._stats = cast(StatsWithStream, stats)

        if self.config.output_path:
            self._save_report()

    def get_result(self) -> ProfileResult:
        if not self._stats:
            raise RuntimeError("Profiling not completed. Use within context manager.")

        total_time = getattr(self._stats, "total_tt", 0.0)
        return ProfileResult(
            cpu_time=total_time,
            report_path=self.config.output_path,
        )

    def _save_report(self) -> None:
        if not self._stats or not self.config.output_path:
            return

        self.config.output_path.parent.mkdir(parents=True, exist_ok=True)
        content = self._generate_report()
        self.config.output_path.write_text(content)

    def _generate_report(self) -> str:
        if not self._stats:
            return ""

        suffix = self.config.output_path.suffix.lower() if self.config.output_path else ".txt"

        if suffix == ".json":
            stats_dict = {}
            stats: dict[tuple[str, int, str], tuple[int, int, float, float, dict[Any, Any]]] = getattr(
                self._stats, "stats", {}
            )
            for func, (cc, nc, tt, ct, _) in stats.items():
                func_name = f"{func[0]}:{func[1]}:{func[2]}"
                stats_dict[func_name] = {
                    "ncalls": nc,
                    "tottime": tt,
                    "percall_tottime": tt / nc if nc > 0 else 0,
                    "cumtime": ct,
                    "percall_cumtime": ct / cc if cc > 0 else 0,
                }
            return json.dumps(stats_dict, indent=2)

        elif suffix == ".html":
            stream = io.StringIO()
            stream.write("<html><body><h1>Profile Report</h1><pre>")
            original_stream = getattr(self._stats, "stream", None)
            self._stats.stream = stream
            self._stats.print_stats(self.config.top_n)
            stream.write("</pre></body></html>")
            if original_stream:
                self._stats.stream = original_stream
            return stream.getvalue()

        else:
            stream = io.StringIO()
            original_stream = getattr(self._stats, "stream", None)
            self._stats.stream = stream
            self._stats.print_stats(self.config.top_n)
            if original_stream:
                self._stats.stream = original_stream
            return stream.getvalue()
