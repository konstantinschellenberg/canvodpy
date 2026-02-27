"""
Auxiliary Data Pipeline for GNSS Processing

Manages downloading, reading, preprocessing, and caching of auxiliary files
(ephemerides, clock, atmospheric corrections, etc.) for RINEX processing.
"""

import threading
from pathlib import Path

import numpy as np
import xarray as xr
from canvod.auxiliary._internal import get_logger
from canvod.auxiliary.clock import ClkFile
from canvod.auxiliary.core.base import AuxFile
from canvod.auxiliary.ephemeris import Sp3File
from canvod.auxiliary.preprocessing import prep_aux_ds
from canvod.readers.matching import MatchedDirs
from canvod.readers.matching.dir_matcher import DataDirMatcher
from canvod.utils.tools import YYYYDOY

_SP3_SUBDIR = Path("01_SP3")
_CLK_SUBDIR = Path("02_CLK")


class AuxDataPipeline:
    """Pipeline for managing auxiliary data files in GNSS processing.

    Handles the complete lifecycle of auxiliary files:
    1. Registration of aux file handlers (Sp3File, ClkFile, etc.)
    2. Downloading/loading files
    3. Preprocessing (sv → sid mapping)
    4. Thread-safe caching for parallel RINEX processing

    Parameters
    ----------
    matched_dirs : MatchedDirs
        Matched directories containing date information (YYYYDOY).

    Attributes
    ----------
    matched_dirs : MatchedDirs
        Matched directories for this processing day.
    _registry : dict[str, dict]
        Registered auxiliary file handlers with metadata.
    _cache : dict[str, xr.Dataset]
        Cache for preprocessed (sid-mapped) datasets.
    _lock : threading.Lock
        Thread lock for concurrent access during parallel processing.
    _logger : Logger
        Logger instance for this pipeline.
    """

    def __init__(self, matched_dirs: MatchedDirs, keep_sids: list[str] | None = None):
        """Initialize the auxiliary data pipeline.

        Parameters
        ----------
        matched_dirs : MatchedDirs
            Matched directories for this processing day.
        keep_sids : list[str] | None
            Optional list of specific SIDs to keep. If None, keeps all possible SIDs.
        """
        self.matched_dirs = matched_dirs
        self.keep_sids = keep_sids
        self._registry: dict[str, dict] = {}
        self._cache: dict[str, xr.Dataset] = {}
        self._lock = threading.Lock()
        self._logger = get_logger(__name__).bind(
            date=self.matched_dirs.yyyydoy.to_str(),
            sid_filtering=len(keep_sids) if keep_sids else "all",
        )

        self._logger.info(
            "aux_pipeline_initialized",
            keep_sids_count=len(keep_sids) if keep_sids else None,
        )

    def register(self, name: str, aux_file: AuxFile, required: bool = False) -> None:
        """Register an auxiliary file handler.

        Parameters
        ----------
        name : str
            Identifier for this aux file (e.g., "ephemerides", "clock").
        aux_file : AuxFile
            Instance of AuxFile subclass (Sp3File, ClkFile, etc.).
        required : bool, default False
            If True, pipeline will fail if this file cannot be loaded.

        Examples
        --------
        >>> pipeline = AuxDataPipeline(matched_dirs=md)
        >>> pipeline.register('ephemerides', Sp3File(...), required=True)
        >>> pipeline.register('clock', ClkFile(...), required=True)
        >>> pipeline.register('ionex', IonexFile(...), required=False)
        """
        if name in self._registry:
            self._logger.warning(
                "aux_file_overwrite",
                name=name,
                old_handler=self._registry[name]["handler"].__class__.__name__,
                new_handler=aux_file.__class__.__name__,
            )

        self._registry[name] = {
            "handler": aux_file,
            "required": required,
            "loaded": False,
        }

        self._logger.info(
            "aux_file_registered",
            name=name,
            handler=aux_file.__class__.__name__,
            required=required,
            file_path=str(aux_file.fpath),
        )

    def load_all(self) -> None:
        """Load all registered auxiliary files.

        Performs two-stage loading:
        1. Download & read → raw xr.Dataset with 'sv' dimension
        2. Preprocess → sid-mapped xr.Dataset (cached)

        Raises
        ------
        RuntimeError
            If a required aux file fails to load.
        """
        import time

        start_time = time.time()
        self._logger.info(
            "aux_load_all_started",
            registered_files=len(self._registry),
            file_names=list(self._registry.keys()),
        )

        loaded_count = 0
        failed_count = 0

        for name, entry in self._registry.items():
            handler = entry["handler"]
            required = entry["required"]

            file_start = time.time()

            try:
                self._logger.info(
                    "aux_file_load_started",
                    name=name,
                    file_path=str(handler.fpath),
                )

                # Stage 1: Download & read raw dataset
                raw_ds = handler.data

                # Stage 2: Preprocess (sv → sid mapping) with keep_sids filter
                preprocessed_ds = prep_aux_ds(raw_ds, keep_sids=self.keep_sids)

                # Cache the preprocessed version
                with self._lock:
                    self._cache[name] = preprocessed_ds

                entry["loaded"] = True
                file_duration = time.time() - file_start

                self._logger.info(
                    "aux_file_load_complete",
                    name=name,
                    duration_seconds=round(file_duration, 2),
                    dataset_size=dict(preprocessed_ds.sizes),
                )
                loaded_count += 1

            except Exception as e:
                file_duration = time.time() - file_start
                self._logger.error(
                    "aux_file_load_failed",
                    name=name,
                    duration_seconds=round(file_duration, 2),
                    error=str(e),
                    exception=type(e).__name__,
                    required=required,
                )
                failed_count += 1

                if required:
                    raise RuntimeError(
                        f"Required auxiliary file '{name}' failed to load: {e}"
                    ) from e
                else:
                    self._logger.warning(
                        "aux_file_optional_skip",
                        name=name,
                        reason="load_failed",
                    )

        duration = time.time() - start_time
        self._logger.info(
            "aux_load_all_complete",
            duration_seconds=round(duration, 2),
            loaded=loaded_count,
            failed=failed_count,
            total=len(self._registry),
        )

    def get(self, name: str) -> xr.Dataset:
        """Get a preprocessed (sid-mapped) auxiliary dataset.

        Thread-safe method for concurrent access during parallel RINEX processing.

        Parameters
        ----------
        name : str
            Name of the registered aux file.

        Returns
        -------
        xr.Dataset
            Preprocessed dataset with 'sid' dimension.

        Raises
        ------
        KeyError
            If aux file not registered.
        ValueError
            If aux file registered but not loaded.

        Examples
        --------
        >>> ephem_ds = pipeline.get('ephemerides')
        >>> clk_ds = pipeline.get('clock')
        """
        if name not in self._registry:
            raise KeyError(
                f"Aux file '{name}' not registered. "
                f"Available: {list(self._registry.keys())}"
            )

        if not self._registry[name]["loaded"]:
            raise ValueError(
                f"Aux file '{name}' registered but not loaded. Call load_all() first."
            )

        with self._lock:
            return self._cache[name]

    def get_ephemerides(self) -> xr.Dataset:
        """Convenience method to get ephemerides dataset."""
        return self.get("ephemerides")

    def get_clock(self) -> xr.Dataset:
        """Convenience method to get clock dataset."""
        return self.get("clock")

    def get_for_time_range(
        self,
        name: str,
        start_time: "np.datetime64",
        end_time: "np.datetime64",
        buffer_minutes: int = 5,
    ) -> xr.Dataset:
        """Get aux data sliced for a specific time range with buffer.

        This is useful when processing individual RINEX files that cover
        only a portion of the day (e.g., 15 minutes). The buffer ensures
        we have enough data for interpolation at the boundaries.

        Parameters
        ----------
        name : str
            Name of the registered aux file.
        start_time : np.datetime64
            Start of the time range.
        end_time : np.datetime64
            End of the time range.
        buffer_minutes : int, default 5
            Minutes to add before/after the range for interpolation buffer.

        Returns
        -------
        xr.Dataset
            Aux dataset sliced to the time range (with buffer).

        Examples
        --------
        >>> # Get ephemerides for a 15-minute RINEX file
        >>> rinex_start = np.datetime64('2024-10-29T00:00:00')
        >>> rinex_end = np.datetime64('2024-10-29T00:15:00')
        >>> ephem_slice = pipeline.get_for_time_range(
        ...     'ephemerides', rinex_start, rinex_end, buffer_minutes=5
        ... )
        """
        import numpy as np

        # Get full day's dataset
        full_ds = self.get(name)

        # Add buffer
        buffer = np.timedelta64(buffer_minutes, "m")
        buffered_start = start_time - buffer
        buffered_end = end_time + buffer

        # Slice the dataset
        if "epoch" in full_ds.sizes:
            sliced_ds = full_ds.sel(epoch=slice(buffered_start, buffered_end))
        else:
            # Fallback if epoch dimension has different name
            time_dim = [
                d for d in full_ds.sizes if "time" in d.lower() or "epoch" in d.lower()
            ]
            if time_dim:
                sliced_ds = full_ds.sel(
                    {time_dim[0]: slice(buffered_start, buffered_end)}
                )
            else:
                self._logger.warning(
                    f"Could not find time dimension in '{name}', returning full dataset"
                )
                sliced_ds = full_ds

        self._logger.debug(
            f"Sliced '{name}' from {buffered_start} to {buffered_end}: "
            f"{dict(sliced_ds.sizes)}"
        )

        return sliced_ds

    def is_loaded(self, name: str) -> bool:
        """Check if an aux file has been loaded."""
        return name in self._registry and self._registry[name]["loaded"]

    def list_registered(self) -> dict[str, dict]:
        """Get information about all registered aux files.

        Returns
        -------
        dict[str, dict]
            Dictionary with aux file names as keys and metadata as values.
        """
        return {
            name: {
                "required": entry["required"],
                "loaded": entry["loaded"],
                "handler_type": type(entry["handler"]).__name__,
            }
            for name, entry in self._registry.items()
        }

    @classmethod
    def create_standard(
        cls,
        matched_dirs: MatchedDirs,
        aux_file_path: Path,
        agency: str | None = None,
        product_type: str | None = None,
        ftp_server: str | None = None,
        user_email: str | None = None,
        keep_sids: list[str] | None = None,
    ) -> "AuxDataPipeline":
        """Factory method to create a standard pipeline with ephemerides and
        clock.

        This is a convenience method that creates a pipeline and registers
        the two required auxiliary files (ephemerides and clock) with
        standard configuration.

        Parameters
        ----------
        matched_dirs : MatchedDirs
            Matched directories containing date information.
        aux_file_path : Path
            Root path for auxiliary files (site's gnss_site_data_root).
        agency : str, optional
            Analysis center code (e.g., "COD"). If None, uses config value.
        product_type : str, optional
            Product type ("final", "rapid"). If None, uses config value.
        ftp_server : str, optional
            FTP server URL. If None, uses config value.
        user_email : str, optional
            Email for authenticated FTP (nasa_earthdata_acc_mail from config).
        keep_sids : list[str] | None, optional
            List of specific SIDs to keep. If None, keeps all possible SIDs.

        Returns
        -------
        AuxDataPipeline
            Configured pipeline with ephemerides and clock registered.

        Examples
        --------
        >>> pipeline = AuxDataPipeline.create_standard(matched_dirs, site_root)
        >>> pipeline.load_all()
        >>> ephem_ds = pipeline.get_ephemerides()
        """
        from canvod.auxiliary.clock import ClkFile
        from canvod.auxiliary.ephemeris import Sp3File
        from canvod.utils.config import load_config

        cfg = load_config()
        aux_cfg = cfg.processing.aux_data

        # Use defaults from config if not provided
        agency = agency or aux_cfg.agency
        product_type = product_type or aux_cfg.product_type
        if ftp_server is None:
            servers = aux_cfg.get_ftp_servers(cfg.nasa_earthdata_acc_mail)
            ftp_server = servers[0][0]

        # Determine aux file paths
        sp3_dir = aux_file_path / _SP3_SUBDIR
        clk_dir = aux_file_path / _CLK_SUBDIR

        # Initialize pipeline
        pipeline = cls(matched_dirs=matched_dirs, keep_sids=keep_sids)

        # Register ephemerides (REQUIRED)
        sp3_file = Sp3File.from_datetime_date(
            date=matched_dirs.yyyydoy.date,
            agency=agency,
            product_type=product_type,
            ftp_server=ftp_server,
            local_dir=sp3_dir,
            user_email=user_email,
        )
        pipeline.register("ephemerides", sp3_file, required=True)

        # Register clock (REQUIRED)
        clk_file = ClkFile.from_datetime_date(
            date=matched_dirs.yyyydoy.date,
            agency=agency,
            product_type=product_type,
            ftp_server=ftp_server,
            local_dir=clk_dir,
            user_email=user_email,
        )
        pipeline.register("clock", clk_file, required=True)

        pipeline._logger.info(
            f"Created standard pipeline with ephemerides and clock "
            f"for {matched_dirs.yyyydoy.to_str()}"
        )

        return pipeline

    def __repr__(self) -> str:
        """String representation showing registered files."""
        loaded_count = sum(1 for e in self._registry.values() if e["loaded"])
        return (
            f"AuxDataPipeline(date={self.matched_dirs.yyyydoy.to_str()}, "
            f"registered={len(self._registry)}, loaded={loaded_count})"
        )


if __name__ == "__main__":
    """
    Example usage of AuxDataPipeline

    This demonstrates how to:
    1. Create a pipeline
    2. Register auxiliary files
    3. Load all files
    4. Access cached data (thread-safe for parallel processing)
    """

    def create_aux_pipeline(
        matched_dirs: MatchedDirs, aux_file_path: Path, user_email: str | None = None
    ) -> AuxDataPipeline:
        """Factory function to create a standard AuxDataPipeline with
        ephemerides and clock.

        Parameters
        ----------
        matched_dirs : MatchedDirs
            Matched directories containing date information.
        aux_file_path : Path
            Root path for auxiliary files (site's gnss_site_data_root).
        user_email : str | None
            NASA Earthdata email for CDDIS authentication.

        Returns
        -------
        AuxDataPipeline
            Configured pipeline with ephemerides and clock registered.
        """
        # Initialize pipeline
        pipeline = AuxDataPipeline(matched_dirs=matched_dirs)

        from canvod.utils.config import load_config as _load_cfg

        _cfg = _load_cfg()
        _aux = _cfg.processing.aux_data
        _servers = _aux.get_ftp_servers(_cfg.nasa_earthdata_acc_mail)
        _ftp = _servers[0][0]

        # Determine aux file paths
        sp3_dir = aux_file_path / _SP3_SUBDIR
        clk_dir = aux_file_path / _CLK_SUBDIR

        # Register ephemerides (REQUIRED)
        sp3_file = Sp3File.from_datetime_date(
            date=matched_dirs.yyyydoy.date,
            agency=_aux.agency,
            product_type=_aux.product_type,
            ftp_server=_ftp,
            local_dir=sp3_dir,
            user_email=user_email,
        )
        pipeline.register("ephemerides", sp3_file, required=True)

        # Register clock (REQUIRED)
        clk_file = ClkFile.from_datetime_date(
            date=matched_dirs.yyyydoy.date,
            agency=_aux.agency,
            product_type=_aux.product_type,
            ftp_server=_ftp,
            local_dir=clk_dir,
            user_email=user_email,
        )
        pipeline.register("clock", clk_file, required=True)

        return pipeline

    """Example usage of the auxiliary data pipeline."""

    # Create matched directories
    matcher = DataDirMatcher.from_root(
        Path(
            "/home/nbader/shares/climers/Studies/"
            "GNSS_Vegetation_Study/05_data/01_Rosalia"
        )
    )
    md = MatchedDirs(
        canopy_data_dir=Path(
            "/home/nbader/Music/testdir/02_canopy/01_GNSS/01_raw/24302"
        ),
        reference_data_dir=Path(
            "/home/nbader/Music/testdir/01_reference/01_GNSS/01_raw/24302"
        ),
        yyyydoy=YYYYDOY.from_str("2024302"),
    )
    """Example usage of the auxiliary data pipeline."""

    # ==========================================
    # METHOD 1: Standard pipeline (recommended)
    # ==========================================
    print("=" * 60)
    print("Creating standard pipeline...")
    print("=" * 60)

    pipeline = AuxDataPipeline.create_standard(
        matched_dirs=md,
        # Optional: customize these if needed
        # aux_file_path=Path('/custom/aux/path'),
        # agency='GFZ',
        # product_type='rapid'
    )

    print("\nRegistered files:")
    for name, info in pipeline.list_registered().items():
        print(f"  {name}: {info}")

    # Load all auxiliary files
    print("\nLoading all files...")
    pipeline.load_all()
    print(f"Pipeline: {pipeline}")

    # ==========================================
    # Access full day's data (for context/caching)
    # ==========================================
    print("\n" + "=" * 60)
    print("Accessing full day's data...")
    print("=" * 60)

    ephem_ds = pipeline.get_ephemerides()
    print(f"\nEphemerides shape: {dict(ephem_ds.sizes)}")
    print(f"Variables: {list(ephem_ds.data_vars)}")
    print(f"Time range: {ephem_ds.epoch.min().values} to {ephem_ds.epoch.max().values}")

    clk_ds = pipeline.get_clock()
    print(f"\nClock shape: {dict(clk_ds.sizes)}")
    print(f"Variables: {list(clk_ds.data_vars)}")

    # ==========================================
    # Slice data for specific RINEX time range
    # ==========================================
    print("\n" + "=" * 60)
    print("Slicing for 15-minute RINEX file...")
    print("=" * 60)

    # Simulate processing a RINEX file that covers 00:00 - 00:15
    rinex_start = np.datetime64("2024-10-29T00:00:00")
    rinex_end = np.datetime64("2024-10-29T00:15:00")

    ephem_slice = pipeline.get_for_time_range(
        "ephemerides",
        rinex_start,
        rinex_end,
        buffer_minutes=5,  # Add 5min buffer for interpolation
    )

    print(f"\nSliced ephemerides shape: {dict(ephem_slice.sizes)}")
    print(
        f"Time range: {ephem_slice.epoch.min().values} to "
        f"{ephem_slice.epoch.max().values}"
    )

    # ==========================================
    # Thread-safe access in parallel context
    # ==========================================
    print("\n" + "=" * 60)
    print("Thread-safe parallel access...")
    print("=" * 60)

    print("""
In your parallel RINEX processing (ThreadPoolExecutor), each thread can:

    def preprocess_rnx(rinex_file, pipeline):
        # Thread-safe access to cached aux data
        ephem_ds = pipeline.get('ephemerides')
        clk_ds = pipeline.get('clock')

        # Or slice for this RINEX file's time range
        start, end = get_rinex_time_range(rinex_file)
        ephem_slice = pipeline.get_for_time_range('ephemerides', start, end)

        # Process RINEX with aux data...

The threading.Lock ensures safe concurrent access!
    """)

    # ==========================================
    # METHOD 2: Manual registration (advanced)
    # ==========================================
    print("\n" + "=" * 60)
    print("Advanced: Manual registration...")
    print("=" * 60)

    pipeline2 = AuxDataPipeline(matched_dirs=md)

    # Manually register files for custom configuration
    from canvod.auxiliary.clock import ClkFile
    from canvod.auxiliary.ephemeris import Sp3File
    from canvod.utils.config import load_config

    config = load_config()
    aux_cfg = config.processing.aux_data
    site_config = next(iter(config.sites.sites.values()))
    root = Path(site_config.gnss_site_data_root)
    user_email = config.nasa_earthdata_acc_mail
    servers = aux_cfg.get_ftp_servers(user_email)
    ftp = servers[0][0]

    sp3_file = Sp3File.from_datetime_date(
        date=md.yyyydoy.date,
        agency=aux_cfg.agency,
        product_type=aux_cfg.product_type,
        ftp_server=ftp,
        local_dir=root / _SP3_SUBDIR,
        user_email=user_email,
        add_velocities=True,  # Custom option!
    )
    pipeline2.register("ephemerides", sp3_file, required=True)

    clk_file = ClkFile.from_datetime_date(
        date=md.yyyydoy.date,
        agency=aux_cfg.agency,
        product_type=aux_cfg.product_type,
        ftp_server=ftp,
        local_dir=root / _CLK_SUBDIR,
        user_email=user_email,
    )
    pipeline2.register("clock", clk_file, required=True)

    # Future: Add optional atmospheric corrections
    # ionex_file = IonexFile.from_datetime_date(...)
    # pipeline2.register('ionosphere', ionex_file, required=False)

    print(f"Manual pipeline: {pipeline2}")
    print("Registered files:", list(pipeline2.list_registered().keys()))
