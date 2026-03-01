"""Default pipeline construction from configuration."""

from canvod.ops.grid import GridAssignment
from canvod.ops.pipeline import Pipeline
from canvod.ops.temporal import TemporalAggregate
from canvod.utils.config.models import PreprocessingConfig


def build_default_pipeline(
    config: PreprocessingConfig | None = None,
) -> Pipeline:
    """Build the default preprocessing pipeline from config.

    Parameters
    ----------
    config : PreprocessingConfig | None
        Explicit config. If ``None``, uses
        :pyclass:`PreprocessingConfig` defaults.

    Returns
    -------
    Pipeline
        Ready-to-call pipeline.
    """
    if config is None:
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
