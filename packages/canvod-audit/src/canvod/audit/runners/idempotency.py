"""Infrastructure: Pipeline idempotency.

Running the pipeline twice on the same input data must produce identical
output. If it doesn't, something is non-deterministic — random seeds,
dictionary ordering, parallel race conditions, or floating-point
accumulation order varying between runs.

Usage::

    from canvod.audit.runners import audit_idempotency

    result = audit_idempotency(
        run1_store="/path/to/first_run",
        run2_store="/path/to/second_run",
    )

    print(result.summary())
"""

from __future__ import annotations

from canvod.audit.core import compare_datasets
from canvod.audit.runners.common import (
    AuditResult,
    find_shared_groups,
    load_group,
    open_store,
)
from canvod.audit.tolerances import ToleranceTier


def audit_idempotency(
    run1_store,
    run2_store,
    *,
    groups=None,
    variables=None,
    tier=ToleranceTier.EXACT,
):
    """Compare two stores produced by running the same pipeline twice.

    Parameters
    ----------
    run1_store : str or Path
        Store from the first pipeline run.
    run2_store : str or Path
        Store from the second pipeline run (same input, same config).
    groups : list of str, optional
        Which groups to compare. If not given, auto-discovers.
    variables : list of str, optional
        Which variables to compare. If not given, compares all shared.
    tier : ToleranceTier
        Default is EXACT. Use NUMERICAL if the pipeline involves
        non-deterministic parallel reductions (e.g. Dask with different
        chunk scheduling).

    Returns
    -------
    AuditResult
    """
    store1 = open_store(run1_store)
    store2 = open_store(run2_store)
    result = AuditResult()

    if groups is None:
        groups = find_shared_groups(store1, store2)
        print(f"Found {len(groups)} shared groups: {groups}")

    for group in groups:
        print(f"Comparing run 1 vs run 2: {group} ...")
        ds1 = load_group(store1, group)
        ds2 = load_group(store2, group)

        r = compare_datasets(
            ds1,
            ds2,
            variables=variables,
            tier=tier,
            label=f"{group}: run 1 vs run 2 (idempotency)",
        )
        result.results[f"idempotency_{group}"] = r

    print()
    print(result.summary())
    return result
