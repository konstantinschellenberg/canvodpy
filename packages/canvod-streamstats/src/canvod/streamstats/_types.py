"""Type definitions and constants for streaming statistics."""

from enum import IntFlag, StrEnum
from typing import NamedTuple


class VariableName(StrEnum):
    """Observable variables tracked by streaming statistics."""

    SNR = "SNR"
    PHASE_VAR = "phase_var"
    CYCLE_SLIP_RATE = "cycle_slip_rate"
    DOPPLER_VAR = "doppler_var"
    PSEUDORANGE_RMS = "pseudorange_rms"
    VOD = "VOD"
    DELTA_SNR = "delta_SNR"


class ReceiverType(StrEnum):
    """Receiver type for statistics indexing."""

    CANOPY = "canopy"
    REFERENCE = "reference"
    DERIVED = "derived"


class CellSignalKey(NamedTuple):
    """Composite key for a streaming accumulator."""

    cell_id: int
    signal_id: str
    variable: str
    receiver_type: str


# Default histogram bin specifications per variable: (low, high, n_bins)
DEFAULT_HISTOGRAM_BINS: dict[str, tuple[float, float, int]] = {
    "SNR": (0.0, 60.0, 120),
    "phase_var": (0.0, 0.1, 100),
    "cycle_slip_rate": (0.0, 1.0, 100),
    "doppler_var": (0.0, 10.0, 100),
    "pseudorange_rms": (0.0, 50.0, 100),
    "VOD": (-1.0, 3.0, 200),
    "delta_SNR": (-30.0, 30.0, 120),
}

# Autocovariance defaults
DEFAULT_AUTOCOVARIANCE_MAX_LAG: int = 1440  # 1 day at 1-min resolution

# RLS defaults
DEFAULT_RLS_FORGETTING_FACTOR: float = 0.999
DEFAULT_RLS_N_FEATURES: int = 3  # [cos(θ), cos²(θ), 1]

# PCA defaults
DEFAULT_PCA_N_COMPONENTS: int = 5
DEFAULT_PCA_N_VARIABLES: int = 14  # 7 vars × 2 receivers

# Scintillation defaults
DEFAULT_S4_WINDOW: int = 60  # 60-second averaging window
DEFAULT_FADE_THRESHOLDS: tuple[float, ...] = (0.5, 0.3, 0.1)  # normalized to RMS
DEFAULT_SPECTRAL_SLOPE_N_FEATURES: int = 2  # [log(f), 1] → intercept + slope

# Goertzel / spectral defaults
DEFAULT_GOERTZEL_FREQUENCIES: tuple[float, ...] = (
    1.0 / 86400.0,  # diurnal (1/day in Hz)
    1.0 / 86164.0,  # sidereal (~23h 56m)
    1.0 / 43200.0,  # semi-diurnal
)
DEFAULT_GOERTZEL_WINDOW: int = 86400  # 1-day window in samples (1-sec resolution)
DEFAULT_MULTITAPER_NW: float = 4.0  # time-bandwidth product
DEFAULT_MULTITAPER_K: int = 7  # number of tapers (2*NW - 1)
DEFAULT_SSA_WINDOW: int = 1440  # SSA embedding dimension (1 day at 1-min)
DEFAULT_SSA_N_COMPONENTS: int = 10  # number of SSA components to retain
SIDEREAL_DAY_SECONDS: float = 86164.0905  # mean sidereal day

# Information-theoretic defaults
DEFAULT_SAMPLE_ENTROPY_M: int = 2  # template length for SampEn
DEFAULT_SAMPLE_ENTROPY_R: float = 0.2  # tolerance (fraction of std)
DEFAULT_PERMUTATION_ORDER: int = 5  # embedding dimension d (d! = 120 patterns)
DEFAULT_PERMUTATION_DELAY: int = 1  # delay τ
DEFAULT_DFA_ORDER: int = 1  # polynomial detrending order
DEFAULT_DFA_MIN_SCALE: int = 10  # minimum segment length
DEFAULT_DFA_MAX_SCALE: int | None = None  # None → N//4
DEFAULT_DFA_N_SCALES: int = 20  # number of log-spaced scales
DEFAULT_TRANSFER_ENTROPY_LAG: int = 1  # prediction horizon
DEFAULT_BIVARIATE_N_BINS: int = 50  # bins per axis for 2D histogram

# Climatology defaults
DEFAULT_DOY_WINDOW: int = 15  # days per DOY bin
DEFAULT_TOD_WINDOW: int = 1  # hours per TOD bin
DAYS_PER_YEAR: float = 365.25  # for DOY → angle conversion

# EWMA / temporal smoothing defaults (§14.5)
DEFAULT_EWMA_HALFLIFE: float = 10.0  # half-life in number of samples
DEFAULT_RUNNING_MEDIAN_WINDOW: int = 5  # sliding window size


class AnomalyClassification(StrEnum):
    """Anomaly severity from standardised z-score."""

    NORMAL = "normal"  # |z| < 1
    MILD = "mild"  # 1 ≤ |z| < 2
    MODERATE = "moderate"  # 2 ≤ |z| < 3
    SEVERE = "severe"  # |z| ≥ 3


# Uncertainty defaults
DEFAULT_COHERENT_INTEGRATION_MS: float = 20.0  # T_c in ms
DEFAULT_NONCOHERENT_AVERAGES: int = 50  # M (1-second averaging)
DEFAULT_ELEVATION_SIGMA_RAD: float = 1.745e-4  # ~0.01° for broadcast ephemerides


class QualityFlag(IntFlag):
    """VOD quality bitmask."""

    GOOD = 0
    LOW_ELEV = 1
    OUTLIER = 2
    CYCLE_SLIP = 4
    RFI = 8


# Bayesian / BOCPD defaults
DEFAULT_BOCPD_MAX_RUN_LENGTH: int = 500  # R: max run length (~20KB state)
DEFAULT_BOCPD_HAZARD_LAMBDA: float = 30.0  # λ: geometric prior timescale (days)
DEFAULT_BOCPD_PRIOR_MU: float = 0.0  # μ₀: NIG prior mean
DEFAULT_BOCPD_PRIOR_KAPPA: float = 1.0  # κ₀: NIG prior precision scaling
DEFAULT_BOCPD_PRIOR_ALPHA: float = 1.0  # α₀: NIG prior shape
DEFAULT_BOCPD_PRIOR_BETA: float = 1.0  # β₀: NIG prior scale

# Mixture model defaults
DEFAULT_MIXTURE_N_COMPONENTS: int = 3  # K: number of Gaussian components
DEFAULT_MIXTURE_MAX_ITER: int = 100  # EM iteration limit
DEFAULT_MIXTURE_TOL: float = 1e-6  # EM convergence tolerance
DEFAULT_MIXTURE_MIN_WEIGHT: float = 1e-8  # minimum component weight

# CAR/ICAR defaults
DEFAULT_CAR_MAX_ITER: int = 200  # iterative solver limit
DEFAULT_CAR_TOL: float = 1e-6  # convergence tolerance

# Robust statistics defaults
DEFAULT_MAD_SCALE_FACTOR: float = 1.4826  # MAD → std for Gaussian (1/Phi^{-1}(3/4))
DEFAULT_TRIM_FRACTION: float = 0.1  # alpha for trimmed mean (trim 10% each tail)
DEFAULT_HUBER_THRESHOLD: float = 1.345  # Huber c (95% Gaussian efficiency)

# Default quantile probabilities (11 levels)
DEFAULT_QUANTILE_PROBS: tuple[float, ...] = (
    0.001,
    0.01,
    0.05,
    0.1,
    0.25,
    0.5,
    0.75,
    0.9,
    0.95,
    0.99,
    0.999,
)
