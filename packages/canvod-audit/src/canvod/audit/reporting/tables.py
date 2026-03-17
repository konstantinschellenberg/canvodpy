"""Table export for audit results: LaTeX, Markdown, Polars."""

from __future__ import annotations

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from canvod.audit.core import ComparisonResult


def to_polars(result: ComparisonResult) -> object:
    """Return per-variable stats as a polars DataFrame."""
    return result.to_polars()


def to_markdown(result: ComparisonResult) -> str:
    """Render per-variable stats as a Markdown table."""
    df = result.to_polars()
    if df.is_empty():
        return "_No variables compared._"

    cols = df.columns
    lines = []
    lines.append("| " + " | ".join(cols) + " |")
    lines.append("| " + " | ".join(["---"] * len(cols)) + " |")

    for row in df.iter_rows(named=True):
        cells = []
        for c in cols:
            v = row[c]
            if isinstance(v, float):
                cells.append(f"{v:.6g}")
            else:
                cells.append(str(v))
        lines.append("| " + " | ".join(cells) + " |")

    return "\n".join(lines)


def to_latex(
    result: ComparisonResult,
    *,
    caption: str = "",
    label: str = "",
) -> str:
    """Render per-variable stats as a LaTeX table.

    Produces a ``tabular`` environment inside a ``table`` float, ready
    to paste into a paper.
    """
    df = result.to_polars()
    if df.is_empty():
        return "% No variables compared."

    cols = df.columns
    col_spec = "l" + "r" * (len(cols) - 1)

    lines = [
        r"\begin{table}[htbp]",
        r"\centering",
        f"\\caption{{{caption or result.label}}}",
    ]
    if label:
        lines.append(f"\\label{{{label}}}")

    lines.append(f"\\begin{{tabular}}{{{col_spec}}}")
    lines.append(r"\toprule")

    # Header
    header = " & ".join(f"\\textbf{{{c}}}" for c in cols) + r" \\"
    lines.append(header)
    lines.append(r"\midrule")

    # Rows
    for row in df.iter_rows(named=True):
        cells = []
        for c in cols:
            v = row[c]
            if isinstance(v, float):
                if abs(v) < 1e-3 or abs(v) > 1e4:
                    cells.append(f"${v:.2e}$")
                else:
                    cells.append(f"{v:.4f}")
            else:
                cells.append(str(v).replace("_", r"\_"))
        lines.append(" & ".join(cells) + r" \\")

    lines.append(r"\bottomrule")
    lines.append(r"\end{tabular}")
    lines.append(r"\end{table}")

    return "\n".join(lines)
