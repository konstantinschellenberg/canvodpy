"""Streaming statistics for GNSS-VOD observations."""

from canvod.streamstats._types import (
    DAYS_PER_YEAR,
    DEFAULT_AUTOCOVARIANCE_MAX_LAG,
    DEFAULT_BIVARIATE_N_BINS,
    DEFAULT_BOCPD_HAZARD_LAMBDA,
    DEFAULT_BOCPD_MAX_RUN_LENGTH,
    DEFAULT_BOCPD_PRIOR_ALPHA,
    DEFAULT_BOCPD_PRIOR_BETA,
    DEFAULT_BOCPD_PRIOR_KAPPA,
    DEFAULT_BOCPD_PRIOR_MU,
    DEFAULT_CAR_MAX_ITER,
    DEFAULT_CAR_TOL,
    DEFAULT_COHERENT_INTEGRATION_MS,
    DEFAULT_DFA_MAX_SCALE,
    DEFAULT_DFA_MIN_SCALE,
    DEFAULT_DFA_N_SCALES,
    DEFAULT_DFA_ORDER,
    DEFAULT_DOY_WINDOW,
    DEFAULT_ELEVATION_SIGMA_RAD,
    DEFAULT_FADE_THRESHOLDS,
    DEFAULT_GOERTZEL_FREQUENCIES,
    DEFAULT_GOERTZEL_WINDOW,
    DEFAULT_HISTOGRAM_BINS,
    DEFAULT_HUBER_THRESHOLD,
    DEFAULT_MAD_SCALE_FACTOR,
    DEFAULT_MIXTURE_MAX_ITER,
    DEFAULT_MIXTURE_MIN_WEIGHT,
    DEFAULT_MIXTURE_N_COMPONENTS,
    DEFAULT_MIXTURE_TOL,
    DEFAULT_MULTITAPER_K,
    DEFAULT_MULTITAPER_NW,
    DEFAULT_NONCOHERENT_AVERAGES,
    DEFAULT_PCA_N_COMPONENTS,
    DEFAULT_PCA_N_VARIABLES,
    DEFAULT_PERMUTATION_DELAY,
    DEFAULT_PERMUTATION_ORDER,
    DEFAULT_QUANTILE_PROBS,
    DEFAULT_RLS_FORGETTING_FACTOR,
    DEFAULT_RLS_N_FEATURES,
    DEFAULT_S4_WINDOW,
    DEFAULT_SAMPLE_ENTROPY_M,
    DEFAULT_SAMPLE_ENTROPY_R,
    DEFAULT_SPECTRAL_SLOPE_N_FEATURES,
    DEFAULT_SSA_N_COMPONENTS,
    DEFAULT_SSA_WINDOW,
    DEFAULT_TOD_WINDOW,
    DEFAULT_TRANSFER_ENTROPY_LAG,
    DEFAULT_TRIM_FRACTION,
    SIDEREAL_DAY_SECONDS,
    AnomalyClassification,
    CellSignalKey,
    QualityFlag,
    ReceiverType,
    VariableName,
)
from canvod.streamstats.accumulators.autocovariance import StreamingAutocovariance
from canvod.streamstats.accumulators.bivariate_histogram import BivariateHistogram
from canvod.streamstats.accumulators.circular import CircularAccumulator
from canvod.streamstats.accumulators.fading import FadingAccumulator
from canvod.streamstats.accumulators.gk_sketch import GKSketch
from canvod.streamstats.accumulators.goertzel import GoertzelAccumulator
from canvod.streamstats.accumulators.histogram import StreamingHistogram
from canvod.streamstats.accumulators.huber_rls import HuberRLS
from canvod.streamstats.accumulators.pca import IncrementalPCA
from canvod.streamstats.accumulators.permutation_entropy import (
    PermutationEntropyAccumulator,
)
from canvod.streamstats.accumulators.rls import RecursiveLeastSquares
from canvod.streamstats.accumulators.scintillation import (
    S4Accumulator,
    nakagami_m_from_s4,
    sigma_phi,
)
from canvod.streamstats.accumulators.spectral_slope import SpectralSlopeAccumulator
from canvod.streamstats.accumulators.welford import WelfordAccumulator
from canvod.streamstats.bayesian.bocpd import BOCPDAccumulator, BOCPDResult
from canvod.streamstats.bayesian.mixture import (
    GaussianMixtureResult,
    fit_gaussian_mixture,
    fit_gaussian_mixture_from_histogram,
)
from canvod.streamstats.bayesian.spatial import (
    CARResult,
    adjacency_from_grid,
    car_smooth,
    icar_smooth,
)
from canvod.streamstats.climatology.anomaly import (
    anomaly_zscore,
    anomaly_zscore_batch,
    classify_anomaly,
    classify_anomaly_batch,
)
from canvod.streamstats.climatology.grid import ClimatologyGrid
from canvod.streamstats.information.dfa import DFAResult, dfa, hurst_exponent
from canvod.streamstats.information.entropy import (
    conditional_entropy,
    joint_entropy,
    shannon_entropy,
    shannon_entropy_from_histogram,
)
from canvod.streamstats.information.mutual_info import (
    MutualInformationResult,
    mutual_information,
    mutual_information_from_histogram,
)
from canvod.streamstats.information.sample_entropy import (
    SampleEntropyResult,
    sample_entropy,
)
from canvod.streamstats.information.transfer_entropy import (
    TransferEntropyResult,
    transfer_entropy,
)
from canvod.streamstats.robust.estimators import mad, robust_std, trimmed_mean
from canvod.streamstats.spectral.emd import EMDResult, emd_decompose
from canvod.streamstats.spectral.lomb_scargle import LombScargleResult, lomb_scargle
from canvod.streamstats.spectral.multitaper import MultitaperResult, multitaper_psd
from canvod.streamstats.spectral.sidereal import (
    SiderealFilterResult,
    sidereal_filter,
)
from canvod.streamstats.spectral.ssa import SSAResult, ssa_decompose
from canvod.streamstats.uncertainty.aggregation import (
    aggregation_uncertainty,
    effective_sample_size,
    effective_sample_size_from_autocovariance,
)
from canvod.streamstats.uncertainty.propagation import (
    sigma_cn0,
    sigma_cn0_batch,
    sigma_delta_snr,
    sigma_delta_snr_batch,
    sigma_transmissivity,
    sigma_transmissivity_batch,
    sigma_vod,
    sigma_vod_batch,
)
