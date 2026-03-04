"""Conditional Autoregressive (CAR) and Intrinsic CAR (ICAR) spatial smoothing.

Offline functions that solve the CAR precision system via scipy.sparse.linalg.spsolve.
Accept raw adjacency matrices — no canvod-grids dependency.
"""

from __future__ import annotations

from dataclasses import dataclass

import numpy as np
from scipy import sparse
from scipy.sparse.linalg import spsolve

from canvod.streamstats._types import DEFAULT_CAR_MAX_ITER, DEFAULT_CAR_TOL


@dataclass(frozen=True)
class CARResult:
    """Result of CAR/ICAR spatial smoothing."""

    smoothed: np.ndarray  # (N,)
    raw: np.ndarray  # (N,)
    residuals: np.ndarray  # raw - smoothed
    spatial_variance: float  # estimated σ²
    n_iterations: int  # 0 for direct solve
    converged: bool
    effective_cells: int  # cells with finite input


def car_smooth(
    values: np.ndarray,
    adjacency: np.ndarray | sparse.spmatrix,
    tau: float = 1.0,
    rho: float = 0.99,
    max_iter: int = DEFAULT_CAR_MAX_ITER,
    tol: float = DEFAULT_CAR_TOL,
) -> CARResult:
    """CAR spatial smoothing via direct sparse solve.

    Solves: (D - ρW + τI)θ = τy

    Parameters
    ----------
    values : array of shape (N,)
        Observed values. NaN cells are filled with global mean for the solve.
    adjacency : array or sparse matrix of shape (N, N)
        Symmetric adjacency / weight matrix W.
    tau : float
        Data precision (higher = trust data more, less smoothing).
    rho : float
        Spatial autocorrelation parameter in (0, 1).
    max_iter : int
        Not used for direct solve, reserved for iterative extensions.
    tol : float
        Not used for direct solve, reserved for iterative extensions.

    Returns
    -------
    CARResult
    """
    values = np.asarray(values, dtype=np.float64).ravel()
    N = len(values)

    # Convert adjacency to sparse CSR
    if not sparse.issparse(adjacency):
        W = sparse.csr_matrix(np.asarray(adjacency, dtype=np.float64))
    else:
        W = sparse.csr_matrix(adjacency, dtype=np.float64)

    # Identify finite cells
    finite_mask = np.isfinite(values)
    effective_cells = int(np.sum(finite_mask))

    # Fill NaN with global mean
    if effective_cells == 0:
        return CARResult(
            smoothed=np.full(N, np.nan),
            raw=values.copy(),
            residuals=np.full(N, np.nan),
            spatial_variance=float("nan"),
            n_iterations=0,
            converged=True,
            effective_cells=0,
        )

    y = values.copy()
    global_mean = np.nanmean(values)
    y[~finite_mask] = global_mean

    # Build precision matrix: Q = D - ρW + τI
    # D = diag(row sums of W)
    d = np.asarray(W.sum(axis=1)).ravel()
    D = sparse.diags(d, format="csr")
    eye = sparse.eye(N, format="csr")

    Q = D - rho * W + tau * eye

    # RHS: τy (zero out NaN positions to not influence solution)
    rhs = tau * y
    rhs[~finite_mask] = tau * global_mean

    # Direct solve
    theta = spsolve(Q, rhs)

    # Spatial variance estimate: σ² = (y - θ)ᵀ(y - θ) / effective_cells
    residuals_finite = y[finite_mask] - theta[finite_mask]
    spatial_var = float(np.sum(residuals_finite**2) / effective_cells)

    residuals = values - theta
    # Preserve NaN in residuals where input was NaN
    residuals[~finite_mask] = np.nan

    return CARResult(
        smoothed=theta,
        raw=values.copy(),
        residuals=residuals,
        spatial_variance=spatial_var,
        n_iterations=0,
        converged=True,
        effective_cells=effective_cells,
    )


def icar_smooth(
    values: np.ndarray,
    adjacency: np.ndarray | sparse.spmatrix,
    tau: float = 1.0,
    max_iter: int = DEFAULT_CAR_MAX_ITER,
    tol: float = DEFAULT_CAR_TOL,
) -> CARResult:
    """ICAR spatial smoothing (CAR with ρ=1).

    Solves: (D - W + τI)θ = τy

    ICAR is the intrinsic autoregressive model where the spatial precision
    is the graph Laplacian. Adding τI ensures the system is non-singular.

    Parameters
    ----------
    values : array of shape (N,)
        Observed values.
    adjacency : array or sparse matrix of shape (N, N)
        Symmetric adjacency / weight matrix W.
    tau : float
        Data precision.
    max_iter : int
        Reserved for iterative extensions.
    tol : float
        Reserved for iterative extensions.

    Returns
    -------
    CARResult
    """
    return car_smooth(
        values=values,
        adjacency=adjacency,
        tau=tau,
        rho=1.0,
        max_iter=max_iter,
        tol=tol,
    )


def adjacency_from_grid(
    cell_ids: np.ndarray,
    neighbor_fn: callable,
) -> sparse.csr_matrix:
    """Build a sparse adjacency matrix from cell IDs and a neighbor function.

    Parameters
    ----------
    cell_ids : array of shape (N,)
        Integer cell identifiers.
    neighbor_fn : callable
        Function mapping a cell_id to an iterable of neighbor cell_ids.
        Only neighbors present in cell_ids are included.

    Returns
    -------
    scipy.sparse.csr_matrix of shape (N, N)
        Symmetric binary adjacency matrix.
    """
    cell_ids = np.asarray(cell_ids).ravel()
    N = len(cell_ids)
    id_to_idx = {int(cid): i for i, cid in enumerate(cell_ids)}

    rows: list[int] = []
    cols: list[int] = []

    for i, cid in enumerate(cell_ids):
        for nb in neighbor_fn(int(cid)):
            j = id_to_idx.get(int(nb))
            if j is not None and j != i:
                rows.append(i)
                cols.append(j)

    data = np.ones(len(rows), dtype=np.float64)
    W = sparse.csr_matrix((data, (rows, cols)), shape=(N, N))

    # Ensure symmetry
    W = W + W.T
    W.data[:] = np.minimum(W.data, 1.0)

    return W
