"""Empirical Mode Decomposition (EMD / EEMD / CEEMDAN).

.. deprecated::
    EMD is marked for removal. It requires the external ``EMD-signal``
    C library, is batch-only (not streaming), and its trend extraction
    functionality is better served by :class:`RecursiveLeastSquares`
    with forgetting factor or :class:`EWMAAccumulator`.
"""

from __future__ import annotations

import warnings
from dataclasses import dataclass

import numpy as np


@dataclass(frozen=True)
class EMDResult:
    """Result of EMD decomposition."""

    imfs: np.ndarray  # (n_imfs, N) intrinsic mode functions
    residual: np.ndarray  # (N,) residual after IMF extraction
    n_imfs: int  # number of IMFs extracted
    instantaneous_frequency: np.ndarray | None  # (n_imfs, N) if computed


def emd_decompose(
    values: np.ndarray,
    method: str = "ceemdan",
    max_imfs: int | None = None,
    compute_instantaneous_frequency: bool = False,
) -> EMDResult:
    """Perform Empirical Mode Decomposition.

    Parameters
    ----------
    values : array
        1-D time series.
    method : str
        ``"emd"``, ``"eemd"``, or ``"ceemdan"`` (default).
    max_imfs : int, optional
        Maximum number of IMFs to extract.  ``None`` lets the algorithm decide.
    compute_instantaneous_frequency : bool
        If ``True``, compute instantaneous frequency via the Hilbert transform.

    Returns
    -------
    EMDResult
    """
    warnings.warn(
        "emd_decompose is deprecated and will be removed in a future release. "
        "Use RecursiveLeastSquares with forgetting factor or EWMAAccumulator "
        "for trend extraction.",
        DeprecationWarning,
        stacklevel=2,
    )
    x = np.asarray(values, dtype=np.float64).ravel()

    # Filter NaN / Inf values
    finite_mask = np.isfinite(x)
    x = x[finite_mask]
    N = len(x)

    if N < 4:
        return EMDResult(
            imfs=np.empty((0, N), dtype=np.float64),
            residual=x.copy() if N > 0 else np.array([], dtype=np.float64),
            n_imfs=0,
            instantaneous_frequency=None,
        )

    decomposer = _get_decomposer(method, max_imfs)
    imfs = decomposer(x)  # (n_imfs, N) or (n_imfs+1, N) depending on library version

    if imfs.ndim == 1:
        imfs = imfs.reshape(1, -1)

    n_imfs = imfs.shape[0]

    # Residual = original - sum of all IMFs
    residual = x - np.sum(imfs, axis=0)

    # Instantaneous frequency via Hilbert transform
    inst_freq = None
    if compute_instantaneous_frequency and n_imfs > 0:
        from scipy.signal import hilbert

        analytic = hilbert(imfs, axis=1)
        phase = np.unwrap(np.angle(analytic), axis=1)
        # Instantaneous frequency = d(phase)/dt / (2*pi)
        inst_freq = np.diff(phase, axis=1) / (2.0 * np.pi)
        # Pad to match original length
        inst_freq = np.concatenate([inst_freq, inst_freq[:, -1:]], axis=1)

    return EMDResult(
        imfs=imfs,
        residual=residual,
        n_imfs=n_imfs,
        instantaneous_frequency=inst_freq,
    )


def _get_decomposer(method: str, max_imfs: int | None):
    """Return a callable that performs EMD decomposition."""
    method = method.lower()

    try:
        import PyEMD  # noqa: F401
    except ImportError:
        msg = (
            "EMD-signal is required for emd_decompose(). "
            "Install with: pip install 'canvod-streamstats[emd]'"
        )
        raise ImportError(msg) from None

    if method == "emd":
        from PyEMD import EMD

        emd = EMD()
        if max_imfs is not None:
            emd.MAX_ITERATION = max_imfs * 100  # rough heuristic

        def _decompose(x):
            return emd.emd(x, max_imf=max_imfs if max_imfs else -1)

        return _decompose

    elif method == "eemd":
        from PyEMD import EEMD

        eemd = EEMD()

        def _decompose(x):
            return eemd.eemd(x, max_imf=max_imfs if max_imfs else -1)

        return _decompose

    elif method == "ceemdan":
        from PyEMD import CEEMDAN

        ceemdan = CEEMDAN()

        def _decompose(x):
            return ceemdan.ceemdan(x, max_imf=max_imfs if max_imfs else -1)

        return _decompose

    else:
        msg = f"Unknown EMD method: {method!r}. Use 'emd', 'eemd', or 'ceemdan'."
        raise ValueError(msg)
