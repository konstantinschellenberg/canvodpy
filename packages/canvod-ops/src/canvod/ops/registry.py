"""Default pipeline construction from configuration."""

from __future__ import annotations

from typing import TYPE_CHECKING

from canvod.ops.grid import GridAssignment
from canvod.ops.pipeline import Pipeline
from canvod.ops.temporal import TemporalAggregate
from canvod.utils.config.models import PreprocessingConfig

if TYPE_CHECKING:
    from canvod.ops.statistics.profile import ProfileRegistry


def build_default_pipeline(
    config: PreprocessingConfig | None = None,
) -> Pipeline:
    """Build the default preprocessing pipeline from config.

    Parameters
    ----------
    config : PreprocessingConfig | None
        Explicit config. If ``None``, attempts to load from the user's
        config files via ``load_config()``. Falls back to
        ``PreprocessingConfig()`` defaults if no config is available.

    Returns
    -------
    Pipeline
        Ready-to-call pipeline.
    """
    if config is None:
        try:
            from canvod.utils.config import load_config

            config = load_config().processing.preprocessing
        except Exception:
            config = PreprocessingConfig()

    pipeline = Pipeline()

    if config.temporal_aggregation.enabled:
        pipeline.add(
            TemporalAggregate(
                freq=config.temporal_aggregation.freq,
                method=config.temporal_aggregation.method,
            )
        )

    if config.grid_assignment.enabled:
        pipeline.add(
            GridAssignment(
                grid_type=config.grid_assignment.grid_type,
                angular_resolution=config.grid_assignment.angular_resolution,
            )
        )

    return pipeline


def build_statistics_pipeline(
    config: PreprocessingConfig | None = None,
    registry: ProfileRegistry | None = None,
    receiver_type: str = "canopy",
    variables: list[str] | None = None,
) -> tuple[Pipeline, ProfileRegistry]:
    """Build a pipeline that includes temporal + grid + UpdateStatistics.

    Parameters
    ----------
    config : PreprocessingConfig | None
        Pipeline config. Loaded from user settings if ``None``.
    registry : ProfileRegistry | None
        Existing registry to update. Creates a new one if ``None``.
    receiver_type : str
        Receiver type label.
    variables : list[str] | None
        Variables to profile. ``None`` → from config or all data vars.

    Returns
    -------
    tuple[Pipeline, ProfileRegistry]
        The pipeline and the registry it will update.
    """
    from canvod.ops.statistics.op import UpdateStatistics
    from canvod.ops.statistics.profile import ProfileRegistry as _PR

    if config is None:
        try:
            from canvod.utils.config import load_config

            config = load_config().processing.preprocessing
        except Exception:
            config = PreprocessingConfig()

    if registry is None:
        gk_eps = config.statistics.gk_epsilon if config.statistics else 0.01
        registry = _PR(gk_epsilon=gk_eps)

    pipeline = build_default_pipeline(config)

    if config.statistics.enabled:
        stats_vars = variables or config.statistics.variables or None
        pipeline.add(
            UpdateStatistics(
                registry=registry,
                receiver_type=receiver_type,
                variables=stats_vars,
            )
        )

    return pipeline, registry
