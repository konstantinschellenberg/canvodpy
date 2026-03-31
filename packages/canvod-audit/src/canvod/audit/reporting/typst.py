"""Typst report generator for ComparisonResult.

Produces a publication-ready ``.typ`` document from a ``ComparisonResult``.

Usage::

    from canvod.audit.reporting.typst import to_typst

    typ_src = to_typst(result, title="Tier 1a: SBF vs RINEX")
    Path("findings.typ").write_text(typ_src)

    # Or write directly:
    to_typst(result, path="findings.typ")
"""

from __future__ import annotations

import math
import subprocess
from datetime import date as _date
from pathlib import Path
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from canvod.audit.core import ComparisonResult


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _esc(s: str) -> str:
    """Escape Typst markup special characters in plain text."""
    return (
        s.replace("\\", "\\\\")
        .replace("#", "\\#")
        .replace("@", "\\@")
        .replace("<", "\\<")
        .replace(">", "\\>")
        .replace("_", "\\_")
        .replace("*", "\\*")
        .replace("[", "\\[")
        .replace("]", "\\]")
    )


def _fmt(v: float, precision: int = 4) -> str:
    """Format a float for a table cell; NaN → em-dash."""
    if math.isnan(v):
        return "---"
    if v == 0.0:
        return "0"
    mag = abs(v)
    if mag != 0 and (mag < 1e-3 or mag >= 1e6):
        return f"{v:.{precision}e}"
    return f"{v:.{precision}g}"


def _pct(v: float) -> str:
    if math.isnan(v):
        return "---"
    return f"{v:.2%}"


# ---------------------------------------------------------------------------
# Main entry point
# ---------------------------------------------------------------------------


def to_typst(
    result: ComparisonResult,
    *,
    title: str | None = None,
    path: str | Path | None = None,
    report_date: str | None = None,
    notes: list[str] | None = None,
    compile: bool = False,
) -> str:
    """Render a ComparisonResult as a Typst source document.

    Parameters
    ----------
    result : ComparisonResult
        The comparison to render.
    title : str, optional
        Document title. Defaults to ``result.label``.
    path : str or Path, optional
        If provided, write the source to this file in addition to returning it.
    report_date : str, optional
        ISO date string for the report. Defaults to today.
    notes : list[str], optional
        Free-text methodological notes rendered in a dedicated section before
        the annotations table.  Use these to document known behavioural
        differences that are expected and do not indicate a bug.
    compile : bool
        If True (and ``path`` is set), run ``typst compile <path>`` after
        writing to produce a PDF alongside the ``.typ`` source.

    Returns
    -------
    str
        Typst source.
    """
    doc_title = title or result.label or "Audit Comparison Report"
    today = report_date or str(_date.today())
    status = "PASSED" if result.passed else "FAILED"
    status_color = "#2d7a2d" if result.passed else "#b03030"

    lines: list[str] = []

    # ── Document preamble ──────────────────────────────────────────────────
    lines += [
        '#set text(font: "New Computer Modern", size: 10pt)',
        '#set page(paper: "a4", margin: (x: 2.5cm, y: 3cm))',
        '#set heading(numbering: "1.")',
        "#set table(stroke: 0.4pt)",
        "",
        f'#let pass-color = rgb("{status_color}")',
        "",
    ]

    # ── Title block ────────────────────────────────────────────────────────
    lines += [
        f"= {_esc(doc_title)}",
        "",
        "#grid(",
        "  columns: (1fr, 1fr, 1fr),",
        f"  [*Tier:* {_esc(result.tier.value)}],",
        f"  [*Status:* #text(fill: pass-color)[*{status}*]],",
        f"  [*Date:* {today}],",
        ")",
        "",
    ]

    # ── Domain / alignment ─────────────────────────────────────────────────
    lines.append("== Domain")
    lines.append("")

    if result.alignment:
        a = result.alignment
        dropped_epochs = a.n_dropped_epochs_a + a.n_dropped_epochs_b
        dropped_sids = a.n_dropped_sids_a + a.n_dropped_sids_b
        lines.append(
            f"Aligned on *{a.n_shared_epochs:,}* epochs "
            f"$times$ *{a.n_shared_sids}* SIDs "
            f"(A: {a.n_epochs_a:,} epochs, {a.n_sids_a} SIDs; "
            f"B: {a.n_epochs_b:,} epochs, {a.n_sids_b} SIDs)."
        )
        if dropped_epochs or dropped_sids:
            lines.append("")
            lines.append(
                f'#text(fill: rgb("#b06000"))[*Warning:* dropped '
                f"{dropped_epochs:,} epochs ({a.n_dropped_epochs_a} from A, "
                f"{a.n_dropped_epochs_b} from B) and "
                f"{dropped_sids} SIDs ({a.n_dropped_sids_a} from A, "
                f"{a.n_dropped_sids_b} from B) during alignment.]"
            )
    else:
        lines.append("No alignment performed (pre-aligned inputs).")

    lines.append("")

    # ── Variable statistics table ──────────────────────────────────────────
    lines.append("== Variable Statistics")
    lines.append("")

    n_vars = len(result.variable_stats)
    n_exact = sum(1 for vs in result.variable_stats.values() if vs.exact_match)
    lines.append(
        f"*{n_vars}* variables compared, "
        f"*{n_exact}* bit-identical, "
        f"*{n_vars - n_exact}* differ."
    )
    lines.append("")

    if result.variable_stats:
        # Table header
        lines += [
            "#table(",
            "  columns: (auto, auto, auto, auto, auto, auto, auto, auto, auto, auto),",
            "  table.header(",
            "    [*Variable*], [*Exact*], [*N compared*], [*Max abs diff*],",
            "    [*RMSE*], [*Bias*], [*p50*], [*p99*], [*NaN% A*], [*NaN% B*],",
            "  ),",
        ]

        for vname, vs in result.variable_stats.items():
            # Inside #table(...) we are in code mode — no # prefix on function calls
            exact_cell = (
                'text(fill: rgb("#2d7a2d"))[✓]'
                if vs.exact_match
                else 'text(fill: rgb("#b03030"))[✗]'
            )
            if vs.n_compared == 0:
                lines.append(
                    f"  [{_esc(vname)}], {exact_cell}, [0], [---], [---], [---], [---], [---], "
                    f"[{_pct(vs.pct_nan_a)}], [{_pct(vs.pct_nan_b)}],"
                )
            else:
                lines.append(
                    f"  [{_esc(vname)}], {exact_cell}, [{vs.n_compared:,}], "
                    f"[{_fmt(vs.max_abs_diff)}], [{_fmt(vs.rmse)}], "
                    f"[{_fmt(vs.bias)}], [{_fmt(vs.p50)}], [{_fmt(vs.p99)}], "
                    f"[{_pct(vs.pct_nan_a)}], [{_pct(vs.pct_nan_b)}],"
                )

        lines.append(")")
        lines.append("")

    # ── Coverage ───────────────────────────────────────────────────────────
    if result.coverage:
        c = result.coverage
        has_coverage_content = (
            c.vars_a_only
            or c.vars_b_only
            or any(
                c.valid_a_only.get(v, 0) or c.valid_b_only.get(v, 0)
                for v in c.valid_both
            )
        )

        if has_coverage_content:
            lines.append("== Coverage")
            lines.append("")

            if c.vars_a_only:
                items = ", ".join(f"`{v}`" for v in c.vars_a_only)
                lines.append(f"*Variables only in A:* {items}")
                lines.append("")
            if c.vars_b_only:
                items = ", ".join(f"`{v}`" for v in c.vars_b_only)
                lines.append(f"*Variables only in B:* {items}")
                lines.append("")

            asymmetric = [
                v
                for v in c.valid_both
                if c.valid_a_only.get(v, 0) or c.valid_b_only.get(v, 0)
            ]
            if asymmetric:
                lines.append("=== Validity Asymmetry")
                lines.append("")
                lines.append(
                    "Cells where one dataset has a valid value and the other is NaN:"
                )
                lines.append("")
                lines += [
                    "#table(",
                    "  columns: (auto, auto, auto, auto, auto),",
                    "  table.header(",
                    "    [*Variable*], [*Both valid*], [*A only*], [*B only*], [*Neither*],",
                    "  ),",
                ]
                for v in asymmetric:
                    lines.append(
                        f"  [{_esc(v)}], [{c.valid_both[v]:,}], "
                        f"[{c.valid_a_only[v]:,}], [{c.valid_b_only[v]:,}], "
                        f"[{c.neither_valid[v]:,}],"
                    )
                lines.append(")")
                lines.append("")

    # ── Methodological notes ───────────────────────────────────────────────
    if notes:
        lines.append("== Methodological Notes")
        lines.append("")
        for note in notes:
            lines.append(f"- {_esc(note)}")
        lines.append("")

    # ── Annotations / failures ─────────────────────────────────────────────
    if result.failures:
        lines.append("== Annotations")
        lines.append("")
        lines.append(
            "Tolerance check annotations (these failures determine the overall pass/fail verdict):"
        )
        lines.append("")
        lines += [
            "#table(",
            "  columns: (auto, 1fr),",
            "  table.header([*Variable*], [*Note*]),",
        ]
        for var, reason in result.failures.items():
            lines.append(f"  [{_esc(var)}], [{_esc(reason)}],")
        lines.append(")")
        lines.append("")

    src = "\n".join(lines)

    if path is not None:
        p = Path(path)
        p.write_text(src, encoding="utf-8")
        if compile:
            result_cp = subprocess.run(
                ["typst", "compile", str(p)],
                capture_output=True,
                text=True,
            )
            if result_cp.returncode != 0:
                raise RuntimeError(f"typst compile failed:\n{result_cp.stderr}")

    return src
