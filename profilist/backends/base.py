from __future__ import annotations

from typing import Protocol, Self

from ..models import ProfileResult


class ProfilerProtocol(Protocol):
    def __enter__(self) -> Self: ...

    def __exit__(self, *args: object) -> None: ...

    def get_result(self) -> ProfileResult: ...
