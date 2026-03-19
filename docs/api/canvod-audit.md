# canvod.audit API Reference

Audit, comparison, and regression verification for canvodpy GNSS-VOD pipelines.

## Core

::: canvod.audit.core
    options:
      members:
        - compare_datasets
        - ComparisonResult
        - AlignmentInfo

## Statistics

::: canvod.audit.stats
    options:
      members:
        - VariableStats
        - compute_variable_stats
        - rmse
        - bias
        - mae
        - max_abs_diff
        - correlation
        - nan_agreement

## Tolerances

::: canvod.audit.tolerances
    options:
      members:
        - Tolerance
        - ToleranceTier
        - get_tolerance
        - TIER_DEFAULTS
        - SCIENTIFIC_DEFAULTS

## Tiers — Internal

::: canvod.audit.tiers.internal
    options:
      members:
        - compare_sbf_vs_rinex
        - compare_ephemeris_sources

## Tiers — Regression

::: canvod.audit.tiers.regression
    options:
      members:
        - freeze_checkpoint
        - compare_against_checkpoint

## Tiers — External

::: canvod.audit.tiers.external
    options:
      members:
        - compare_vs_gnssvod

## Reporting — Tables

::: canvod.audit.reporting.tables
    options:
      members:
        - to_polars
        - to_markdown
        - to_latex

## Reporting — Figures

::: canvod.audit.reporting.figures
    options:
      members:
        - plot_diff_histogram
        - plot_scatter
        - plot_summary_dashboard
