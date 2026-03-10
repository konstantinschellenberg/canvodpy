"""Audit runners — ready-to-use comparison pipelines.

Each runner takes store paths, runs comparisons, prints progress, and
returns an ``AuditResult`` with ``.passed``, ``.summary()``, ``.to_polars()``.

Tier 0 — canvodpy self-consistency:
    ``audit_vs_gnssvodpy``       canvodpy vs gnssvodpy (predecessor)
    ``audit_api_levels``         L1 vs L2 vs L3 vs L4

Tier 1 — internal consistency:
    ``audit_sbf_vs_rinex``       SBF vs RINEX from the same receivers
    ``audit_ephemeris_sources``  broadcast vs agency ephemeris

Tier 2 — regression:
    ``freeze_checkpoint``        save a known-good output
    ``audit_regression``         check current output against checkpoints

Tier 3 — external:
    ``audit_vs_gnssvod``         canvodpy vs gnssvod (Humphrey et al.)

Infrastructure:
    ``audit_store_round_trip``       write → read → compare
    ``audit_temporal_chunking``      chunked vs monolithic processing
    ``audit_idempotency``            first run vs second run
    ``audit_constellation_filter``   GPS-only vs all-constellation subset
"""

from canvod.audit.runners.api_levels import audit_api_levels
from canvod.audit.runners.common import AuditResult, load_group, open_store
from canvod.audit.runners.constellation_filter import audit_constellation_filter
from canvod.audit.runners.ephemeris import audit_ephemeris_sources
from canvod.audit.runners.idempotency import audit_idempotency
from canvod.audit.runners.regression import audit_regression, freeze_checkpoint
from canvod.audit.runners.round_trip import audit_store_round_trip
from canvod.audit.runners.sbf_vs_rinex import audit_sbf_vs_rinex
from canvod.audit.runners.temporal_chunking import audit_temporal_chunking
from canvod.audit.runners.vs_gnssvod import audit_vs_gnssvod, gnssvod_df_to_xarray
from canvod.audit.runners.vs_gnssvodpy import audit_vs_gnssvodpy

__all__ = [
    "AuditResult",
    "audit_api_levels",
    "audit_constellation_filter",
    "audit_ephemeris_sources",
    "audit_idempotency",
    "audit_regression",
    "audit_sbf_vs_rinex",
    "audit_store_round_trip",
    "audit_temporal_chunking",
    "audit_vs_gnssvod",
    "audit_vs_gnssvodpy",
    "freeze_checkpoint",
    "gnssvod_df_to_xarray",
    "load_group",
    "open_store",
]
