from __future__ import annotations

import functools
from typing import Any, Callable, Literal, ParamSpec, Protocol, Self, TypeVar, cast

from .models import ProfileResult

P = ParamSpec("P")
R = TypeVar("R")

ProfilerType = Literal["cprofile", "scalene"]


class ProfilerProtocol(Protocol):
    def __enter__(self) -> Self: ...

    def __exit__(self, *args: object) -> None: ...

    def get_result(self) -> ProfileResult: ...


ProfilerConstructor = TypeVar("ProfilerConstructor", bound=Callable[..., ProfilerProtocol])


class ProfilerFactory:
    _profilers: dict[str, Callable[..., ProfilerProtocol]] = {}

    @classmethod
    def register(cls, name: str, profiler_class: Callable[..., ProfilerProtocol]) -> None:
        cls._profilers[name] = profiler_class

    @classmethod
    def create(cls, backend: str, config: object | None = None) -> ProfilerProtocol:
        if backend not in cls._profilers:
            available = ", ".join(cls._profilers.keys())
            raise ValueError(f"Unknown backend: {backend}. Available: {available}")
        profiler_class = cls._profilers[backend]
        return profiler_class(config) if config else profiler_class()

    @classmethod
    def available_backends(cls) -> list[str]:
        return list(cls._profilers.keys())


def profile(
    backend: ProfilerType = "cprofile",
    **kwargs: object,
) -> Callable[[Callable[P, R]], Callable[P, R]]:
    def decorator(func: Callable[P, R]) -> Callable[P, R]:
        @functools.wraps(func)
        def wrapper(*args: P.args, **func_kwargs: P.kwargs) -> R:
            from .models import CProfileConfig

            if backend == "cprofile":
                config = CProfileConfig(name=func.__name__, **cast(Any, kwargs))
                profiler = ProfilerFactory.create(backend, config)
                with profiler:
                    result = func(*args, **func_kwargs)
                profile_result = profiler.get_result()
                if not config.silent:
                    print(profile_result.summary)
                return result
            else:
                raise ValueError(f"Decorator not supported for {backend}. Use CProfileProfiler directly.")

        return wrapper

    return decorator


class Profiler:
    def __init__(self, backend: ProfilerType = "cprofile", **kwargs: object) -> None:
        from .models import CProfileConfig

        if backend != "cprofile":
            raise ValueError(
                "Only 'cprofile' backend supported in context manager. For Scalene, use ScaleneProfiler.profile_script()"
            )

        self.backend = backend
        self.config = CProfileConfig(**cast(Any, kwargs))
        self._profiler: ProfilerProtocol | None = None
        self.result: ProfileResult | None = None

    def __enter__(self) -> Self:
        self._profiler = ProfilerFactory.create(self.backend, self.config)
        self._profiler.__enter__()
        return self

    def __exit__(self, *args: object) -> None:
        if self._profiler:
            self._profiler.__exit__(*args)
            self.result = self._profiler.get_result()
