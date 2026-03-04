"""Incremental PCA accumulator using Brand (2006) SVD update."""

from __future__ import annotations

import numpy as np

from canvod.streamstats._types import (
    DEFAULT_PCA_N_COMPONENTS,
    DEFAULT_PCA_N_VARIABLES,
)


class IncrementalPCA:
    """Streaming PCA via incremental SVD (Brand 2006).

    Maintains a truncated SVD basis (U, sigma) and running mean.
    Fully vectorized: all updates use NumPy linalg (SVD, QR).

    State layout: [d, k, count, *mean(d), *sigma(k), *U.ravel(d*k)]
    Total size: 3 + d + k + d*k
    """

    __slots__ = ("_U", "_count", "_d", "_k", "_mean", "_sigma")

    def __init__(
        self,
        n_variables: int = DEFAULT_PCA_N_VARIABLES,
        n_components: int = DEFAULT_PCA_N_COMPONENTS,
    ) -> None:
        if n_components < 1:
            msg = f"n_components must be >= 1, got {n_components}"
            raise ValueError(msg)
        if n_variables < n_components:
            msg = (
                f"n_variables ({n_variables}) must be >= n_components ({n_components})"
            )
            raise ValueError(msg)
        self._d = int(n_variables)
        self._k = int(n_components)
        self._U = np.zeros((self._d, self._k), dtype=np.float64)
        self._sigma = np.zeros(self._k, dtype=np.float64)
        self._mean = np.zeros(self._d, dtype=np.float64)
        self._count = 0

    def update_batch(self, X: np.ndarray) -> None:
        """Incorporate a batch of observations using Brand (2006) incremental SVD.

        Parameters
        ----------
        X : array-like, shape (B, d)
            Batch of observation vectors.
        """
        X = np.asarray(X, dtype=np.float64)
        if X.ndim == 1:
            X = X.reshape(1, -1)
        if X.shape[1] != self._d:
            msg = f"Expected X with {self._d} columns, got {X.shape[1]}"
            raise ValueError(msg)

        # Filter NaN rows
        nan_mask = np.any(np.isnan(X), axis=1)
        X = X[~nan_mask]
        B = X.shape[0]
        if B == 0:
            return

        # Update mean
        n_old = self._count
        n_new = n_old + B
        batch_mean = np.mean(X, axis=0)

        if n_old == 0:
            new_mean = batch_mean
        else:
            new_mean = (n_old * self._mean + B * batch_mean) / n_new

        # Center data
        X_centered = X - new_mean  # (B, d)

        if n_old == 0:
            # Cold start: full SVD, truncate to k
            # X_centered.T is (d, B)
            if B >= self._d:
                # More samples than dimensions — use covariance approach
                U_full, s_full, _ = np.linalg.svd(X_centered.T, full_matrices=False)
            else:
                U_full, s_full, _ = np.linalg.svd(X_centered.T, full_matrices=False)

            k = min(self._k, len(s_full))
            self._U[:, :k] = U_full[:, :k]
            self._sigma[:k] = s_full[:k]
            self._mean = new_mean
            self._count = n_new
            return

        # Warm path: Brand (2006) incremental SVD update
        # Mean correction: add a pseudo-row for the mean shift
        mean_correction = np.sqrt(n_old * B / n_new) * (self._mean - batch_mean)
        X_aug = np.vstack([X_centered, mean_correction.reshape(1, -1)])  # (B+1, d)

        U = self._U  # (d, k)
        sigma = self._sigma  # (k,)

        # Project new data onto existing basis
        M = U.T @ X_aug.T  # (k, B+1) — projections
        P = X_aug.T - U @ M  # (d, B+1) — residuals

        # QR factorization of residual
        Q, R_p = np.linalg.qr(P, mode="reduced")  # Q: (d, B+1), R_p: (B+1, B+1)
        r = Q.shape[1]  # rank of residual (may be < B+1)

        # Build augmented matrix
        # Top: [diag(sigma), M]  — shape (k, k + B+1)
        # Bottom: [0, R_p]       — shape (r, k + B+1)
        top = np.zeros((len(sigma), len(sigma) + M.shape[1]), dtype=np.float64)
        top[:, : len(sigma)] = np.diag(sigma)
        top[:, len(sigma) :] = M

        bottom = np.zeros((r, len(sigma) + M.shape[1]), dtype=np.float64)
        bottom[:, len(sigma) :] = R_p

        augmented = np.vstack([top, bottom])  # (k + r, k + B+1)

        # SVD of small augmented matrix
        U_aug, s_aug, _ = np.linalg.svd(augmented, full_matrices=False)

        # Truncate to k components
        k = min(self._k, len(s_aug))

        # Reconstruct basis: [U | Q] @ U_aug[:, :k]
        UQ = np.hstack([U, Q])  # (d, k + r)
        self._U = UQ @ U_aug[:, :k]
        self._sigma[:k] = s_aug[:k]
        if k < self._k:
            self._sigma[k:] = 0.0
        self._mean = new_mean
        self._count = n_new

    def merge(self, other: IncrementalPCA) -> IncrementalPCA:
        """Merge another PCA accumulator by treating its basis as a pseudo-batch.

        Returns self.
        """
        if other._count == 0:
            return self
        if self._count == 0:
            self._d = other._d
            self._k = other._k
            self._U = other._U.copy()
            self._sigma = other._sigma.copy()
            self._mean = other._mean.copy()
            self._count = other._count
            return self

        if self._d != other._d or self._k != other._k:
            msg = (
                f"Cannot merge PCA with different dims: "
                f"({self._d},{self._k}) vs ({other._d},{other._k})"
            )
            raise ValueError(msg)

        # Treat other's basis (U·diag(σ)) as pseudo-batch
        # Each column of U_other * sigma_other represents a direction
        # We construct pseudo-observations from the basis
        pseudo = (other._U * other._sigma[None, :]).T  # (k, d) — k pseudo-observations

        # Weight the merge mean
        n_total = self._count + other._count
        merged_mean = (self._count * self._mean + other._count * other._mean) / n_total

        # Center pseudo-observations around merged mean
        X_pseudo = pseudo - merged_mean  # (k, d)

        # Add mean correction row
        mean_correction = np.sqrt(self._count * other._count / n_total) * (
            self._mean - other._mean
        )
        X_aug = np.vstack([X_pseudo, mean_correction.reshape(1, -1)])

        U = self._U
        sigma = self._sigma

        M = U.T @ X_aug.T
        P = X_aug.T - U @ M
        Q, R_p = np.linalg.qr(P, mode="reduced")
        r = Q.shape[1]

        top = np.zeros((len(sigma), len(sigma) + M.shape[1]), dtype=np.float64)
        top[:, : len(sigma)] = np.diag(sigma)
        top[:, len(sigma) :] = M
        bottom = np.zeros((r, len(sigma) + M.shape[1]), dtype=np.float64)
        bottom[:, len(sigma) :] = R_p
        augmented = np.vstack([top, bottom])

        U_aug, s_aug, _ = np.linalg.svd(augmented, full_matrices=False)
        k = min(self._k, len(s_aug))

        UQ = np.hstack([U, Q])
        self._U = UQ @ U_aug[:, :k]
        self._sigma[:k] = s_aug[:k]
        if k < self._k:
            self._sigma[k:] = 0.0
        self._mean = merged_mean
        self._count = n_total

        return self

    # --- Properties ---

    @property
    def count(self) -> int:
        return self._count

    @property
    def components(self) -> np.ndarray:
        """Principal component directions, shape (k, d). Rows are components."""
        return self._U.T.copy()

    @property
    def singular_values(self) -> np.ndarray:
        """Singular values, shape (k,)."""
        return self._sigma.copy()

    @property
    def explained_variance(self) -> np.ndarray:
        """Variance explained by each component, shape (k,)."""
        if self._count < 2:
            return np.full(self._k, np.nan)
        return self._sigma**2 / (self._count - 1)

    @property
    def explained_variance_ratio(self) -> np.ndarray:
        """Fraction of total variance explained by each component."""
        ev = self.explained_variance
        if self._count < 2:
            return np.full(self._k, np.nan)
        total = np.sum(ev)
        if total == 0.0:
            return np.full(self._k, np.nan)
        return ev / total

    @property
    def mean(self) -> np.ndarray:
        """Running mean, shape (d,)."""
        return self._mean.copy()

    # --- Serialization ---

    def to_array(self) -> np.ndarray:
        """Serialize to flat array: [d, k, count, *mean(d), *sigma(k), *U.ravel(d*k)].

        Shape: (3 + d + k + d*k,)
        """
        d, k = self._d, self._k
        arr = np.empty(3 + d + k + d * k, dtype=np.float64)
        arr[0] = float(d)
        arr[1] = float(k)
        arr[2] = float(self._count)
        arr[3 : 3 + d] = self._mean
        arr[3 + d : 3 + d + k] = self._sigma
        arr[3 + d + k :] = self._U.ravel()
        return arr

    @classmethod
    def from_array(cls, arr: np.ndarray) -> IncrementalPCA:
        """Restore from serialized array."""
        data = np.asarray(arr, dtype=np.float64)
        d = int(data[0])
        k = int(data[1])
        obj = cls.__new__(cls)
        obj._d = d
        obj._k = k
        obj._count = int(data[2])
        obj._mean = data[3 : 3 + d].copy()
        obj._sigma = data[3 + d : 3 + d + k].copy()
        obj._U = data[3 + d + k : 3 + d + k + d * k].reshape(d, k).copy()
        return obj
