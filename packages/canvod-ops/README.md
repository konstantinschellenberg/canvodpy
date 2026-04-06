# canvod-ops

Composable preprocessing operations pipeline for GNSS-Transmissometry.

Part of the [canVODpy](https://github.com/nfb2021/canvodpy) ecosystem.

## Overview

`canvod-ops` provides a modular `Op`-based pipeline for applying preprocessing
steps to GNSS datasets. Operations are composable and chainable via `Pipeline`.

## Key components

| Component | Purpose |
|---|---|
| `Op` | Abstract base class for all operations |
| `GridAssignment` | Assigns satellite observations to equal-area grid cells |
| `Pipeline` | Chains operations and returns `PipelineResult` |

## Installation

```bash
uv pip install canvod-ops
```

## Quick Start

```python
from canvod.ops import Pipeline, GridAssignment

grid = create_hemigrid(grid_type="equal_area", resolution=2.0)
pipeline = Pipeline([GridAssignment(grid)])
result = pipeline.run(ds)
```

## Documentation

[Full documentation](https://nfb2021.github.io/canvodpy/packages/ops/overview/)

## License

Apache License 2.0
