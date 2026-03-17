"""Publication-quality figures for audit results."""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

import numpy as np

if TYPE_CHECKING:
    from canvod.audit.core import ComparisonResult


def plot_diff_histogram(
    ds_a: Any,
    ds_b: Any,
    variable: str,
    *,
    ax: Any = None,
    bins: int = 100,
    title: str | None = None,
) -> Any:
    """Histogram of element-wise differences (a - b) for a single variable.

    Returns a matplotlib Figure.
    """
    import matplotlib.pyplot as plt

    a = ds_a[variable].values.ravel().astype(np.float64)
    b = ds_b[variable].values.ravel().astype(np.float64)
    mask = np.isfinite(a) & np.isfinite(b)
    diff = a[mask] - b[mask]

    if ax is None:
        fig, ax = plt.subplots(figsize=(8, 4))
    else:
        fig = ax.get_figure()

    ax.hist(diff, bins=bins, color="#5D7D5B", edgecolor="#375D3B", alpha=0.8)
    ax.axvline(0, color="#C75050", linewidth=1, linestyle="--")
    ax.set_xlabel(f"Difference ({variable})")
    ax.set_ylabel("Count")
    ax.set_title(title or f"Distribution of differences — {variable}")

    # Annotate with stats
    _text = f"mean={np.mean(diff):.2e}\nstd={np.std(diff):.2e}\nmax|diff|={np.max(np.abs(diff)):.2e}"
    ax.text(
        0.98,
        0.95,
        _text,
        transform=ax.transAxes,
        ha="right",
        va="top",
        fontsize=8,
        family="monospace",
        bbox={"boxstyle": "round", "facecolor": "#E1E6B9", "alpha": 0.8},
    )

    fig.tight_layout()
    return fig


def plot_scatter(
    ds_a: Any,
    ds_b: Any,
    variable: str,
    *,
    ax: Any = None,
    title: str | None = None,
    label_a: str = "Dataset A",
    label_b: str = "Dataset B",
    max_points: int = 50000,
) -> Any:
    """Scatter plot of variable values: a vs b with 1:1 line.

    Returns a matplotlib Figure.
    """
    import matplotlib.pyplot as plt

    a = ds_a[variable].values.ravel().astype(np.float64)
    b = ds_b[variable].values.ravel().astype(np.float64)
    mask = np.isfinite(a) & np.isfinite(b)
    a, b = a[mask], b[mask]

    # Subsample for plotting performance
    if len(a) > max_points:
        idx = np.random.default_rng(42).choice(len(a), max_points, replace=False)
        a, b = a[idx], b[idx]

    if ax is None:
        fig, ax = plt.subplots(figsize=(6, 6))
    else:
        fig = ax.get_figure()

    ax.scatter(b, a, s=1, alpha=0.3, color="#5D7D5B")

    # 1:1 line
    lims = [min(a.min(), b.min()), max(a.max(), b.max())]
    ax.plot(lims, lims, "k--", linewidth=0.8, label="1:1")

    ax.set_xlabel(f"{label_b} — {variable}")
    ax.set_ylabel(f"{label_a} — {variable}")
    ax.set_title(title or f"{variable}: {label_a} vs {label_b}")
    ax.set_aspect("equal")
    ax.legend(loc="upper left")

    fig.tight_layout()
    return fig


def plot_summary_dashboard(
    result: ComparisonResult,
    *,
    figsize: tuple[float, float] = (12, 6),
) -> Any:
    """Multi-panel summary of a ComparisonResult.

    Bar chart of RMSE per variable + pass/fail indicators.
    Returns a matplotlib Figure.
    """
    import matplotlib.pyplot as plt

    variables = list(result.variable_stats.keys())
    if not variables:
        fig, ax = plt.subplots()
        ax.text(0.5, 0.5, "No variables compared", ha="center", va="center")
        return fig

    rmses = [result.variable_stats[v].rmse for v in variables]
    passed = [v not in result.failures for v in variables]
    colors = ["#5D7D5B" if p else "#C75050" for p in passed]

    fig, (ax1, ax2) = plt.subplots(
        1, 2, figsize=figsize, gridspec_kw={"width_ratios": [2, 1]}
    )

    # RMSE bar chart
    ax1.barh(variables, rmses, color=colors)
    ax1.set_xlabel("RMSE")
    ax1.set_title(f"{result.label} — RMSE per variable")
    ax1.invert_yaxis()

    # NaN agreement chart
    nan_agree = [result.variable_stats[v].nan_agreement_rate for v in variables]
    ax2.barh(variables, nan_agree, color="#ABC8A4")
    ax2.set_xlabel("NaN agreement rate")
    ax2.set_xlim(0, 1.05)
    ax2.set_title("NaN pattern agreement")
    ax2.invert_yaxis()

    status = "PASSED" if result.passed else "FAILED"
    fig.suptitle(f"{result.label}  [{status}]", fontweight="bold")
    fig.tight_layout()
    return fig
