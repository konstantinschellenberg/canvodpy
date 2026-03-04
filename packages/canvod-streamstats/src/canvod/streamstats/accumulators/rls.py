"""Recursive Least Squares (RLS) accumulator with forgetting factor."""

from __future__ import annotations

import numpy as np

from canvod.streamstats._types import (
    DEFAULT_RLS_FORGETTING_FACTOR,
    DEFAULT_RLS_N_FEATURES,
)


class RecursiveLeastSquares:
    """Streaming linear regression via Recursive Least Squares.

    Maintains coefficient vector beta and covariance matrix P with
    exponential forgetting factor lambda. Fully vectorized batch updates
    via block RLS (weighted normal equations).

    State layout: [p, lambda, count, *beta(p), *P.ravel(p²)]
    Total size: 3 + p + p²
    """

    __slots__ = ("_P", "_beta", "_count", "_lambda", "_p")

    def __init__(
        self,
        n_features: int = DEFAULT_RLS_N_FEATURES,
        forgetting_factor: float = DEFAULT_RLS_FORGETTING_FACTOR,
        initial_covariance: float = 1000.0,
    ) -> None:
        if n_features < 1:
            msg = f"n_features must be >= 1, got {n_features}"
            raise ValueError(msg)
        if not 0.0 < forgetting_factor <= 1.0:
            msg = f"forgetting_factor must be in (0, 1], got {forgetting_factor}"
            raise ValueError(msg)
        self._p = int(n_features)
        self._lambda = float(forgetting_factor)
        self._beta = np.zeros(self._p, dtype=np.float64)
        self._P = np.eye(self._p, dtype=np.float64) * initial_covariance
        self._count = 0

    def update(self, x: np.ndarray, y: float) -> None:
        """Incorporate a single observation (x, y).

        Parameters
        ----------
        x : array-like, shape (p,)
            Feature vector.
        y : float
            Target value.
        """
        x = np.asarray(x, dtype=np.float64).ravel()
        if np.any(np.isnan(x)) or np.isnan(y):
            return
        if len(x) != self._p:
            msg = f"Expected x of length {self._p}, got {len(x)}"
            raise ValueError(msg)

        lam = self._lambda
        Px = self._P @ x
        denom = lam + x @ Px
        K = Px / denom

        error = y - x @ self._beta
        self._beta = self._beta + K * error
        self._P = (self._P - np.outer(K, x @ self._P)) / lam
        self._count += 1

    def update_batch(self, X: np.ndarray, y: np.ndarray) -> None:
        """Incorporate a batch of observations using block RLS.

        Fully vectorized: computes weighted normal equations and a single
        p×p matrix inversion. Zero Python loops.

        Parameters
        ----------
        X : array-like, shape (N, p)
            Feature matrix.
        y : array-like, shape (N,)
            Target vector.
        """
        X = np.asarray(X, dtype=np.float64)
        y = np.asarray(y, dtype=np.float64).ravel()

        if X.ndim == 1:
            X = X.reshape(1, -1)
        if X.shape[1] != self._p:
            msg = f"Expected X with {self._p} columns, got {X.shape[1]}"
            raise ValueError(msg)
        if X.shape[0] != len(y):
            msg = f"X has {X.shape[0]} rows but y has {len(y)} elements"
            raise ValueError(msg)

        # Filter NaN rows
        nan_mask = np.any(np.isnan(X), axis=1) | np.isnan(y)
        X = X[~nan_mask]
        y = y[~nan_mask]
        N = len(y)
        if N == 0:
            return

        lam = self._lambda

        # Forgetting weights: λ^(N-1-i) for i = 0..N-1
        W = lam ** np.arange(N - 1, -1, -1, dtype=np.float64)

        # Weighted normal equations
        WX = W[:, None] * X  # (N, p)
        XtWX = X.T @ WX  # (p, p)
        XtWy = X.T @ (W * y)  # (p,)

        # Update covariance: P_inv_new = λ^N * P_inv + X'WX
        lam_N = lam**N
        P_inv_old = np.linalg.inv(self._P)
        P_inv_new = lam_N * P_inv_old + XtWX
        self._P = np.linalg.inv(P_inv_new)

        # Update coefficients
        self._beta = self._P @ (lam_N * P_inv_old @ self._beta + XtWy)
        self._count += N

    def merge(self, other: RecursiveLeastSquares) -> RecursiveLeastSquares:
        """Merge another RLS accumulator — right-biased (takes higher count).

        RLS is inherently sequential per key, so merging takes the accumulator
        with more observations. Returns self.
        """
        if other._count == 0:
            return self
        if self._count == 0 or other._count > self._count:
            self._p = other._p
            self._lambda = other._lambda
            self._beta = other._beta.copy()
            self._P = other._P.copy()
            self._count = other._count
        return self

    # --- Properties ---

    @property
    def count(self) -> int:
        return self._count

    @property
    def beta(self) -> np.ndarray:
        """Coefficient vector of shape (p,)."""
        return self._beta.copy()

    @property
    def P(self) -> np.ndarray:
        """Covariance matrix of shape (p, p)."""
        return self._P.copy()

    @property
    def n_features(self) -> int:
        return self._p

    @property
    def forgetting_factor(self) -> float:
        return self._lambda

    # --- Serialization ---

    def to_array(self) -> np.ndarray:
        """Serialize to flat array: [p, lambda, count, *beta(p), *P.ravel(p²)].

        Shape: (3 + p + p²,)
        """
        p = self._p
        arr = np.empty(3 + p + p * p, dtype=np.float64)
        arr[0] = float(self._p)
        arr[1] = self._lambda
        arr[2] = float(self._count)
        arr[3 : 3 + p] = self._beta
        arr[3 + p :] = self._P.ravel()
        return arr

    @classmethod
    def from_array(cls, arr: np.ndarray) -> RecursiveLeastSquares:
        """Restore from serialized array."""
        data = np.asarray(arr, dtype=np.float64)
        p = int(data[0])
        obj = cls.__new__(cls)
        obj._p = p
        obj._lambda = float(data[1])
        obj._count = int(data[2])
        obj._beta = data[3 : 3 + p].copy()
        obj._P = data[3 + p : 3 + p + p * p].reshape(p, p).copy()
        return obj
