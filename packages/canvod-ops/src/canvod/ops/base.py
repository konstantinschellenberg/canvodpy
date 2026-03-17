"""Base abstractions for preprocessing operations."""

from abc import ABC, abstractmethod
from dataclasses import asdict, dataclass
from typing import Any

import xarray as xr


@dataclass(frozen=True)
class OpResult:
    """Immutable record of a single preprocessing operation."""

    op_name: str
    parameters: dict[str, Any]
    input_shape: dict[str, int]
    output_shape: dict[str, int]
    duration_seconds: float
    notes: str = ""

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


class Op(ABC):
    """Abstract base class for a preprocessing operation.

    Each ``Op`` is a callable carrying config set at construction time.
    At call time it is a pure ``Dataset -> (Dataset, OpResult)`` transform.
    """

    @property
    @abstractmethod
    def name(self) -> str: ...

    @abstractmethod
    def __call__(self, ds: xr.Dataset) -> tuple[xr.Dataset, OpResult]: ...
