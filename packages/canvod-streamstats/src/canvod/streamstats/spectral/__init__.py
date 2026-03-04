"""Spectral and decomposition batch operations for GNSS-VOD analysis."""

from canvod.streamstats.spectral.emd import EMDResult, emd_decompose
from canvod.streamstats.spectral.lomb_scargle import LombScargleResult, lomb_scargle
from canvod.streamstats.spectral.multitaper import MultitaperResult, multitaper_psd
from canvod.streamstats.spectral.sidereal import (
    SiderealFilterResult,
    sidereal_filter,
)
from canvod.streamstats.spectral.ssa import SSAResult, ssa_decompose
