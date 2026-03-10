"""Infrastructure: Temporal chunking consistency.

Does processing data in different temporal chunks produce the same result?

For example, processing days 1-3 then 4-7 should give the same output as
processing all 7 days at once. If it doesn't, there is state leaking
between processing batches — a serious bug.

Usage::

    from canvod.audit.runners import audit_temporal_chunking

    result = audit_temporal_chunking(
        monolithic_store="/path/to/all_7_days",
        chunked_store="/path/to/days_processed_separately",
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


def audit_temporal_chunking(
    monolithic_store,
    chunked_store,
    *,
    groups=None,
    variables=None,
):
    """Compare a monolithic store against one built from temporal chunks.

    Both stores should have been produced from the same input data with
    the same configuration — the only difference is how the processing
    was batched in time.

    Parameters
    ----------
    monolithic_store : str or Path
        Store produced by processing all dates at once.
    chunked_store : str or Path
        Store produced by processing dates in separate batches.
    groups : list of str, optional
        Which groups to compare. If not given, auto-discovers.
    variables : list of str, optional
        Which variables to compare. If not given, compares all shared.

    Returns
    -------
    AuditResult
    """
    store_mono = open_store(monolithic_store)
    store_chunk = open_store(chunked_store)
    result = AuditResult()

    if groups is None:
        groups = find_shared_groups(store_mono, store_chunk)
        print(f"Found {len(groups)} shared groups: {groups}")

    for group in groups:
        print(f"Comparing monolithic vs chunked: {group} ...")
        ds_mono = load_group(store_mono, group)
        ds_chunk = load_group(store_chunk, group)

        print(f"  Monolithic: {len(ds_mono.epoch)} epochs, {len(ds_mono.sid)} sids")
        print(f"  Chunked:    {len(ds_chunk.epoch)} epochs, {len(ds_chunk.sid)} sids")

        r = compare_datasets(
            ds_mono,
            ds_chunk,
            variables=variables,
            tier=ToleranceTier.EXACT,
            label=f"{group}: monolithic vs chunked processing",
        )
        result.results[f"chunking_{group}"] = r

    print()
    print(result.summary())
    return result
