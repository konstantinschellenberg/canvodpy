# canvod-ops

Generic preprocessing operations pipeline (composable, modular).

## Key modules

| Module | Purpose |
|---|---|
| `base.py` | `Op` ABC — all operations inherit this |
| `grid.py` | `GridAssignment(Op)` — assigns satellite observations to grid cells |
| `pipeline.py` | `Pipeline` — chains operations, `OpResult` / `PipelineResult` |

## Pattern

```python
from canvod.ops import Pipeline, GridAssignment, TemporalAggregate

pipeline = Pipeline([GridAssignment(grid), TemporalAggregate(interval="1h")])
result = pipeline.run(ds)
```

## Testing

```bash
uv run pytest packages/canvod-ops/tests/
```
