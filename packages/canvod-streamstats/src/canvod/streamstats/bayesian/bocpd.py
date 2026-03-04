"""Streaming Bayesian Online Changepoint Detection (BOCPD).

Implements Adams & MacKay (2007) with a Normal-Inverse-Gamma (NIG)
conjugate model as the underlying predictive model (UPM). The hazard
function is constant-rate geometric: H(r) = 1/λ.

Memory footprint: ~20KB at max_run_length=500 (5 float64 arrays of R+1).
"""

from __future__ import annotations

import math
from dataclasses import dataclass

import numpy as np
from scipy.special import gammaln


@dataclass(frozen=True)
class BOCPDResult:
    """Snapshot of BOCPD state at the current time step."""

    map_run_length: int
    changepoint_prob: float
    predictive_mean: float
    predictive_std: float
    n_observations: int


class BOCPDAccumulator:
    """Streaming BOCPD with Normal-Inverse-Gamma UPM.

    State: _run_length_dist (R+1,), _suf (R+1, 4) for κ,μ,α,β per run length.
    Memory: ~20KB at R=500.
    """

    __slots__ = (
        "_alpha0",
        "_beta0",
        "_count",
        "_hazard_lambda",
        "_kappa0",
        "_max_R",
        "_mu0",
        "_run_length_dist",
        "_suf",
    )

    # Indices into _suf columns
    _KAPPA = 0
    _MU = 1
    _ALPHA = 2
    _BETA = 3

    def __init__(
        self,
        max_run_length: int = 500,
        hazard_lambda: float = 30.0,
        mu0: float = 0.0,
        kappa0: float = 1.0,
        alpha0: float = 1.0,
        beta0: float = 1.0,
    ) -> None:
        self._max_R = max_run_length
        self._hazard_lambda = hazard_lambda
        self._mu0 = mu0
        self._kappa0 = kappa0
        self._alpha0 = alpha0
        self._beta0 = beta0
        self._count = 0

        R1 = max_run_length + 1
        self._run_length_dist = np.zeros(R1, dtype=np.float64)
        self._run_length_dist[0] = 1.0  # start with run length 0

        self._suf = np.zeros((R1, 4), dtype=np.float64)
        self._suf[:, self._KAPPA] = kappa0
        self._suf[:, self._MU] = mu0
        self._suf[:, self._ALPHA] = alpha0
        self._suf[:, self._BETA] = beta0

    def update(self, x: float) -> None:
        """Incorporate a single observation. O(R). Skips NaN."""
        if math.isnan(x):
            return

        R1 = self._max_R + 1
        h = 1.0 / self._hazard_lambda

        # Active range: only run lengths with non-zero probability
        active = min(R1, self._count + 1)

        # --- 1. Predictive probabilities (Student-t, active range only) ---
        kappa = self._suf[:active, self._KAPPA]
        mu = self._suf[:active, self._MU]
        alpha = self._suf[:active, self._ALPHA]
        beta = self._suf[:active, self._BETA]

        df = 2.0 * alpha
        scale2 = beta * (kappa + 1.0) / (alpha * kappa)
        scale2 = np.maximum(scale2, 1e-300)
        z = (x - mu) ** 2 / scale2

        log_pdf = (
            gammaln(0.5 * (df + 1.0))
            - gammaln(0.5 * df)
            - 0.5 * np.log(df * np.pi * scale2)
            - 0.5 * (df + 1.0) * np.log1p(z / df)
        )
        pred = np.exp(log_pdf)

        # --- 2. Growth and changepoint probabilities ---
        rl_active = self._run_length_dist[:active]
        joint = rl_active * pred
        cp = np.sum(joint * h)
        growth = joint * (1.0 - h)

        # Shift growth probabilities: r → r+1
        new_rl = np.zeros(R1, dtype=np.float64)
        new_rl[0] = cp
        if active < R1:
            # All growth fits: growth[0..active-1] → new_rl[1..active]
            new_rl[1 : active + 1] = growth
        else:
            # Truncation: fold last growth into position R1-1
            new_rl[1:R1] = growth[: R1 - 1]
            new_rl[R1 - 1] += growth[R1 - 1]

        # Normalise
        total = np.sum(new_rl)
        if total > 0.0:
            new_rl /= total
        self._run_length_dist = new_rl

        # --- 3. NIG conjugate update (active range only) ---
        # kappa, mu, alpha, beta are views into self._suf; compute updates
        # into new arrays before writing back (avoid aliasing).
        kappa_new = kappa + 1.0
        mu_new = (kappa * mu + x) / kappa_new
        alpha_new = alpha + 0.5
        beta_new = beta + 0.5 * kappa * (x - mu) ** 2 / kappa_new

        # Shift sufficient statistics into a new array:
        # r=0 resets to prior, r=1..new_active are updated from old r-1
        new_suf = self._suf  # reuse storage (will overwrite)
        # r=0: prior
        new_suf[0, self._KAPPA] = self._kappa0
        new_suf[0, self._MU] = self._mu0
        new_suf[0, self._ALPHA] = self._alpha0
        new_suf[0, self._BETA] = self._beta0

        if active < R1:
            # Write updated values at shifted positions (no overlap since
            # kappa_new etc. are independent copies, not views)
            new_suf[1 : active + 1, self._KAPPA] = kappa_new
            new_suf[1 : active + 1, self._MU] = mu_new
            new_suf[1 : active + 1, self._ALPHA] = alpha_new
            new_suf[1 : active + 1, self._BETA] = beta_new
        else:
            new_suf[1:R1, self._KAPPA] = kappa_new[: R1 - 1]
            new_suf[1:R1, self._MU] = mu_new[: R1 - 1]
            new_suf[1:R1, self._ALPHA] = alpha_new[: R1 - 1]
            new_suf[1:R1, self._BETA] = beta_new[: R1 - 1]

        self._count += 1

    def update_batch(self, values: np.ndarray) -> None:
        """Incorporate an array of observations sequentially."""
        flat = np.asarray(values, dtype=np.float64).ravel()
        for x in flat:
            self.update(x)

    @property
    def result(self) -> BOCPDResult:
        """Current BOCPD result snapshot."""
        if self._count == 0:
            return BOCPDResult(
                map_run_length=0,
                changepoint_prob=0.0,
                predictive_mean=float("nan"),
                predictive_std=float("nan"),
                n_observations=0,
            )
        r = self.map_run_length
        kappa = self._suf[r, self._KAPPA]
        mu = self._suf[r, self._MU]
        alpha = self._suf[r, self._ALPHA]
        beta = self._suf[r, self._BETA]

        pred_mean = mu
        if alpha > 1.0:
            pred_var = beta * (kappa + 1.0) / (alpha * kappa) * (alpha / (alpha - 1.0))
        else:
            pred_var = float("inf")
        pred_std = math.sqrt(pred_var) if pred_var >= 0 else float("nan")

        return BOCPDResult(
            map_run_length=r,
            changepoint_prob=self.changepoint_prob,
            predictive_mean=float(pred_mean),
            predictive_std=float(pred_std),
            n_observations=self._count,
        )

    @property
    def run_length_distribution(self) -> np.ndarray:
        """Copy of the run-length probability vector (R+1,)."""
        return self._run_length_dist.copy()

    @property
    def changepoint_prob(self) -> float:
        """P(r_t = 0): probability that the most recent observation is a changepoint."""
        if self._count == 0:
            return 0.0
        return float(self._run_length_dist[0])

    @property
    def map_run_length(self) -> int:
        """Mode of P(r_t): most probable run length."""
        if self._count == 0:
            return 0
        return int(np.argmax(self._run_length_dist))

    @property
    def count(self) -> int:
        return self._count

    def to_array(self) -> np.ndarray:
        """Serialize state to a 1-D float64 array.

        Layout: [max_R, hazard_lambda, mu0, kappa0, alpha0, beta0, count,
                 run_length_dist(R+1), suf(R+1, 4) flattened].
        Total length: 7 + 5*(R+1).
        """
        R1 = self._max_R + 1
        header = np.array(
            [
                float(self._max_R),
                self._hazard_lambda,
                self._mu0,
                self._kappa0,
                self._alpha0,
                self._beta0,
                float(self._count),
            ],
            dtype=np.float64,
        )
        return np.concatenate([header, self._run_length_dist, self._suf.ravel()])

    @classmethod
    def from_array(cls, arr: np.ndarray) -> BOCPDAccumulator:
        """Restore from a serialized array."""
        data = np.asarray(arr, dtype=np.float64)
        max_R = int(data[0])
        R1 = max_R + 1

        obj = cls.__new__(cls)
        obj._max_R = max_R
        obj._hazard_lambda = float(data[1])
        obj._mu0 = float(data[2])
        obj._kappa0 = float(data[3])
        obj._alpha0 = float(data[4])
        obj._beta0 = float(data[5])
        obj._count = int(data[6])

        offset = 7
        obj._run_length_dist = data[offset : offset + R1].copy()
        offset += R1
        obj._suf = data[offset : offset + R1 * 4].reshape(R1, 4).copy()

        return obj

    def merge(self, other: BOCPDAccumulator) -> BOCPDAccumulator:
        """BOCPD is inherently sequential — no algebraic merge exists."""
        raise NotImplementedError(
            "BOCPD is inherently sequential and cannot be merged. "
            "Process observations in order with update() or update_batch()."
        )

    def _predictive_pdf(self, x: float) -> np.ndarray:
        """Student-t predictive density for each run length (vectorised).

        Student-t with df=2α, loc=μ, scale²=β(κ+1)/(ακ).
        Uses gammaln to avoid overflow.
        """
        kappa = self._suf[:, self._KAPPA]
        mu = self._suf[:, self._MU]
        alpha = self._suf[:, self._ALPHA]
        beta = self._suf[:, self._BETA]

        df = 2.0 * alpha
        scale2 = beta * (kappa + 1.0) / (alpha * kappa)

        # Avoid division by zero for scale2=0
        scale2 = np.maximum(scale2, 1e-300)

        z = (x - mu) ** 2 / scale2

        # log Student-t: gammaln((df+1)/2) - gammaln(df/2)
        #                - 0.5*log(df*pi*scale2)
        #                - ((df+1)/2)*log(1 + z/df)
        log_pdf = (
            gammaln(0.5 * (df + 1.0))
            - gammaln(0.5 * df)
            - 0.5 * np.log(df * np.pi * scale2)
            - 0.5 * (df + 1.0) * np.log1p(z / df)
        )

        return np.exp(log_pdf)
