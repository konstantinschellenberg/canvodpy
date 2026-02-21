"""High-level public API for canvodpy.

This module provides the user-friendly API that wraps proven gnssvodpy logic.
Three levels of API:
1. Convenience functions - process_date(), calculate_vod()
2. Object-oriented - Site, Pipeline classes
3. Low-level - Direct access to canvod.* subpackages

Examples
--------
Level 1 - Simple (one-liners):
    >>> from canvodpy import process_date, calculate_vod
    >>> data = process_date("Rosalia", "2025001")
    >>> vod = calculate_vod("Rosalia", "canopy_01", "reference_01", "2025001")

Level 2 - Object-oriented (more control):
    >>> from canvodpy import Site, Pipeline
    >>> site = Site("Rosalia")
    >>> pipeline = site.pipeline()
    >>> data = pipeline.process_date("2025001")

Level 3 - Low-level (full control):
    >>> from canvod.store import GnssResearchSite
    >>> from canvodpy.processor.pipeline_orchestrator import PipelineOrchestrator
    >>> # Direct access to internals

"""

from __future__ import annotations

from typing import TYPE_CHECKING

# Lazy imports to avoid circular dependencies

if TYPE_CHECKING:
    from collections.abc import Generator

    import xarray as xr
    from canvod.store import MyIcechunkStore


class Site:
    """User-friendly wrapper around GnssResearchSite.

    Provides a clean, modern API for site management while using
    the proven GnssResearchSite implementation internally.

    Parameters
    ----------
    name : str
        Site name from configuration (e.g., "Rosalia")

    Attributes
    ----------
    name : str
        Site name
    receivers : dict
        All configured receivers
    active_receivers : dict
        Only active receivers
    vod_analyses : dict
        Configured VOD analysis pairs
    rinex_store
        Access to RINEX data store
    vod_store
        Access to VOD results store

    Examples
    --------
    >>> site = Site("Rosalia")
    >>> print(site.receivers)
    {'canopy_01': {...}, 'reference_01': {...}}

    >>> # Create pipeline
    >>> pipeline = site.pipeline()

    >>> # Access stores
    >>> site.rinex_store.list_groups()

    """

    def __init__(self, name: str) -> None:
        # Lazy import to avoid circular dependency
        from canvod.store import GnssResearchSite

        # Use proven implementation
        self._site = GnssResearchSite(name)
        self.name = name

    @property
    def receivers(self) -> dict:
        """Get all configured receivers."""
        return self._site.receivers

    @property
    def active_receivers(self) -> dict:
        """Get only active receivers."""
        return self._site.active_receivers

    @property
    def vod_analyses(self) -> dict:
        """Get configured VOD analysis pairs."""
        return self._site.active_vod_analyses

    @property
    def rinex_store(self) -> MyIcechunkStore:
        """Access RINEX data store."""
        return self._site.rinex_store

    @property
    def vod_store(self) -> MyIcechunkStore:
        """Access VOD results store."""
        return self._site.vod_store

    def pipeline(
        self,
        keep_vars: list[str] | None = None,
        aux_agency: str = "COD",
        n_workers: int = 12,
        dry_run: bool = False,
        reader: str = "rinex3",
    ) -> Pipeline:
        """Create a processing pipeline for this site.

        Parameters
        ----------
        keep_vars : list[str], optional
            RINEX variables to keep (default: KEEP_RNX_VARS from globals)
        aux_agency : str, default "COD"
            Analysis center for auxiliary data (COD, ESA, GFZ, JPL)
        n_workers : int, default 12
            Number of parallel workers
        dry_run : bool, default False
            If True, simulate processing without execution
        reader : str, default "rinex3"
            Reader to use: ``"rinex3"`` or ``"sbf"``

        Returns
        -------
        Pipeline
            Configured pipeline for this site

        Examples
        --------
        >>> site = Site("Rosalia")
        >>> pipeline = site.pipeline(aux_agency="ESA", n_workers=8)
        >>> data = pipeline.process_date("2025001")

        >>> # SBF binary files
        >>> pipeline = site.pipeline(reader="sbf")

        """
        return Pipeline(
            site=self,
            keep_vars=keep_vars,
            aux_agency=aux_agency,
            n_workers=n_workers,
            dry_run=dry_run,
            reader=reader,
        )

    def __repr__(self) -> str:
        n_receivers = len(self.active_receivers)
        n_analyses = len(self.vod_analyses)
        return f"Site('{self.name}', receivers={n_receivers}, analyses={n_analyses})"

    def __str__(self) -> str:
        return f"GNSS Site: {self.name}"


class Pipeline:
    """User-friendly wrapper around PipelineOrchestrator.

    Provides a clean API for processing workflows while using
    proven orchestrator logic internally.

    Parameters
    ----------
    site : Site or str
        Site object or site name
    keep_vars : list[str], optional
        RINEX variables to keep
    aux_agency : str, default "COD"
        Analysis center for auxiliary data
    n_workers : int, default 12
        Number of parallel workers
    dry_run : bool, default False
        If True, simulate without execution

    Examples
    --------
    >>> # From site object
    >>> site = Site("Rosalia")
    >>> pipeline = Pipeline(site)

    >>> # Or directly from name
    >>> pipeline = Pipeline("Rosalia")

    >>> # Process single date
    >>> data = pipeline.process_date("2025001")

    >>> # Process range
    >>> for date, datasets in pipeline.process_range("2025001", "2025007"):
    ...     print(f"Processed {date}")

    """

    def __init__(
        self,
        site: Site | str,
        keep_vars: list[str] | None = None,
        aux_agency: str = "COD",
        n_workers: int = 12,
        dry_run: bool = False,
        reader: str = "rinex3",
    ) -> None:
        # Handle both Site object and string
        if isinstance(site, str):
            site = Site(site)

        self.site = site
        if keep_vars is None:
            from canvod.utils.config import load_config

            keep_vars = load_config().processing.processing.keep_rnx_vars
        self.keep_vars = keep_vars
        self.aux_agency = aux_agency
        self.n_workers = n_workers
        self.dry_run = dry_run

        # Setup logging
        from canvodpy.logging import get_logger

        self.log = get_logger(__name__).bind(
            site=site.name,
            component="pipeline",
        )

        # Lazy import to avoid circular dependency
        from canvodpy.orchestrator import PipelineOrchestrator

        # Use proven orchestrator implementation
        self._orchestrator = PipelineOrchestrator(
            site=site._site,
            n_max_workers=n_workers,
            dry_run=dry_run,
            reader_name=reader,
        )

        self.log.info(
            "pipeline_initialized",
            aux_agency=aux_agency,
            n_workers=n_workers,
            keep_vars=len(self.keep_vars),
            dry_run=dry_run,
        )

    def process_date(self, date: str) -> dict[str, xr.Dataset]:
        """Process RINEX data for one date.

        Parameters
        ----------
        date : str
            Date in YYYYDOY format (e.g., "2025001" for Jan 1, 2025)

        Returns
        -------
        dict[str, xr.Dataset]
            Processed datasets for each receiver

        Examples
        --------
        >>> pipeline = Pipeline("Rosalia")
        >>> data = pipeline.process_date("2025001")
        >>> print(data.keys())
        dict_keys(['canopy_01', 'canopy_02', 'reference_01'])

        >>> # Access individual receiver
        >>> canopy_data = data['canopy_01']
        >>> print(canopy_data.dims)
        Dimensions: (epoch: 2880, sv: 32, ...)

        """
        log = self.log.bind(date=date)
        log.info("date_processing_started")

        # Use proven orchestrator logic
        for _date_key, datasets, _timing in self._orchestrator.process_by_date(
            keep_vars=self.keep_vars,
            start_from=date,
            end_at=date,
        ):
            log.info(
                "date_processing_complete",
                receivers=len(datasets),
                receiver_names=list(datasets.keys()),
            )
            return datasets  # Return first (only) date

        log.warning("no_data_processed", date=date)
        return {}  # No data processed

    def process_range(
        self,
        start: str,
        end: str,
    ) -> Generator[tuple[str, dict[str, xr.Dataset]], None, None]:
        """Process RINEX data for a date range.

        Parameters
        ----------
        start : str
            Start date (YYYYDOY)
        end : str
            End date (YYYYDOY)

        Yields
        ------
        tuple[str, dict[str, xr.Dataset]]
            (date_key, datasets) for each processed date

        Examples
        --------
        >>> pipeline = Pipeline("Rosalia")
        >>> for date, datasets in pipeline.process_range("2025001", "2025007"):
        ...     print(f"Processed {date}: {len(datasets)} receivers")
        Processed 2025001: 3 receivers
        Processed 2025002: 3 receivers
        ...

        """
        # Use proven orchestrator logic
        for date_key, datasets, _timing in self._orchestrator.process_by_date(
            keep_vars=self.keep_vars,
            start_from=start,
            end_at=end,
        ):
            yield date_key, datasets

    def calculate_vod(
        self,
        canopy: str,
        reference: str,
        date: str,
    ) -> xr.Dataset:
        """Calculate VOD for a receiver pair.

        Parameters
        ----------
        canopy : str
            Canopy receiver name (e.g., "canopy_01")
        reference : str
            Reference receiver name (e.g., "reference_01")
        date : str
            Date in YYYYDOY format

        Returns
        -------
        xr.Dataset
            VOD analysis results

        Examples
        --------
        >>> pipeline = Pipeline("Rosalia")
        >>> vod = pipeline.calculate_vod("canopy_01", "reference_01", "2025001")
        >>> print(vod.vod.mean().values)
        0.42

        """
        log = self.log.bind(date=date, canopy=canopy, reference=reference)
        log.info("vod_calculation_started")

        try:
            # Load processed data from stores
            canopy_data = self.site.rinex_store.read_group(canopy, date=date)
            ref_data = self.site.rinex_store.read_group(reference, date=date)

            # Lazy import to avoid circular dependency
            from canvod.vod import VODCalculator

            # Use proven VOD calculator
            calculator = VODCalculator()
            vod_results = calculator.compute(canopy_data, ref_data)

            # Store results
            analysis_name = f"{canopy}_vs_{reference}"
            self.site.vod_store.write_group(analysis_name, vod_results)

            log.info(
                "vod_calculation_complete",
                analysis=analysis_name,
                vod_mean=float(vod_results.vod.mean().values)
                if "vod" in vod_results
                else None,
            )
            return vod_results
        except Exception as e:
            log.error(
                "vod_calculation_failed",
                error=str(e),
                exception=type(e).__name__,
            )
            raise

    def preview(self) -> dict:
        """Preview processing plan without execution.

        Returns
        -------
        dict
            Summary of dates, receivers, and files

        Examples
        --------
        >>> pipeline = Pipeline("Rosalia")
        >>> plan = pipeline.preview()
        >>> print(f"Total files: {plan['total_files']}")

        """
        return self._orchestrator.preview_processing_plan()

    def __repr__(self) -> str:
        return (
            f"Pipeline(site='{self.site.name}', "
            f"keep_vars={len(self.keep_vars)} vars, "
            f"workers={self.n_workers})"
        )


# ============================================================================
# Level 1 API: Convenience Functions
# ============================================================================


def process_date(
    site: str,
    date: str,
    keep_vars: list[str] | None = None,
    aux_agency: str = "COD",
    n_workers: int = 12,
) -> dict[str, xr.Dataset]:
    """Process RINEX data for one date (convenience function).

    This is the simplest way to process GNSS data - just provide
    the site name and date.

    Parameters
    ----------
    site : str
        Site name (e.g., "Rosalia")
    date : str
        Date in YYYYDOY format (e.g., "2025001")
    keep_vars : list[str], optional
        RINEX variables to keep (default: KEEP_RNX_VARS)
    aux_agency : str, default "COD"
        Analysis center for auxiliary data
    n_workers : int, default 12
        Number of parallel workers

    Returns
    -------
    dict[str, xr.Dataset]
        Processed datasets for each receiver

    Examples
    --------
    >>> from canvodpy import process_date
    >>> data = process_date("Rosalia", "2025001")
    >>> print(data.keys())
    dict_keys(['canopy_01', 'canopy_02', 'reference_01'])

    >>> # With custom settings
    >>> data = process_date(
    ...     "Rosalia",
    ...     "2025001",
    ...     keep_vars=["C1C", "L1C"],
    ...     aux_agency="ESA"
    ... )

    """
    pipeline = Pipeline(
        site=site,
        keep_vars=keep_vars,
        aux_agency=aux_agency,
        n_workers=n_workers,
    )
    return pipeline.process_date(date)


def calculate_vod(
    site: str,
    canopy: str,
    reference: str,
    date: str,
    keep_vars: list[str] | None = None,
    aux_agency: str = "COD",
) -> xr.Dataset:
    """Calculate VOD for a receiver pair (convenience function).

    This is the simplest way to calculate VOD - just provide
    site, receivers, and date.

    Parameters
    ----------
    site : str
        Site name
    canopy : str
        Canopy receiver name
    reference : str
        Reference receiver name
    date : str
        Date in YYYYDOY format
    keep_vars : list[str], optional
        RINEX variables to keep
    aux_agency : str, default "COD"
        Analysis center

    Returns
    -------
    xr.Dataset
        VOD analysis results

    Examples
    --------
    >>> from canvodpy import calculate_vod
    >>> vod = calculate_vod(
    ...     site="Rosalia",
    ...     canopy="canopy_01",
    ...     reference="reference_01",
    ...     date="2025001"
    ... )
    >>> print(vod.vod.mean().values)
    0.42

    """
    pipeline = Pipeline(
        site=site,
        keep_vars=keep_vars,
        aux_agency=aux_agency,
    )
    return pipeline.calculate_vod(canopy, reference, date)


def preview_processing(site: str) -> dict:
    """Preview processing plan for a site (convenience function).

    Parameters
    ----------
    site : str
        Site name

    Returns
    -------
    dict
        Processing plan summary

    Examples
    --------
    >>> from canvodpy import preview_processing
    >>> plan = preview_processing("Rosalia")
    >>> print(f"Total files: {plan['total_files']}")
    Total files: 8640

    """
    pipeline = Pipeline(site, dry_run=True)
    return pipeline.preview()
