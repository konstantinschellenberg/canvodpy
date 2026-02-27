"""Abstract base class for GNSS data readers.

Defines interface that all readers (RINEX v3, RINEX v2, future formats)
must implement to ensure compatibility with downstream pipeline:
- VOD calculation (canvod-vod)
- Storage (canvod-store / MyIcechunkStore)
- Grid operations (canvod-grids)
"""

from abc import ABC, abstractmethod
from collections.abc import Iterator
from datetime import datetime
from pathlib import Path
from typing import ClassVar

import xarray as xr
from pydantic import BaseModel, ConfigDict


class DatasetStructureValidator(BaseModel):
    """Validates xarray.Dataset structure for pipeline compatibility.

    All readers must produce Datasets that pass this validation
    to ensure compatibility with downstream VOD and storage operations.

    Notes
    -----
    This is a Pydantic `BaseModel` with `arbitrary_types_allowed=True`.

    """

    model_config = ConfigDict(arbitrary_types_allowed=True)

    dataset: xr.Dataset

    def validate_dimensions(self) -> None:
        """Validate required dimensions exist.

        Returns
        -------
        None

        Raises
        ------
        ValueError
            If required dimensions (epoch, sid) are missing.

        """
        required_dims = {"epoch", "sid"}
        missing_dims = required_dims - set(self.dataset.dims)
        if missing_dims:
            msg = f"Missing required dimensions: {missing_dims}"
            raise ValueError(msg)

    def validate_coordinates(self) -> None:
        """Validate required coordinates exist and have correct types.

        Returns
        -------
        None

        Raises
        ------
        ValueError
            If required coordinates are missing or have incorrect dtypes.

        """
        required_coords = {
            "epoch": "datetime64[ns]",
            "sid": "object",  # string
            "sv": "object",
            "system": "object",
            "band": "object",
            "code": "object",
            "freq_center": "float32",
            "freq_min": "float32",
            "freq_max": "float32",
        }

        for coord, expected_dtype in required_coords.items():
            if coord not in self.dataset.coords:
                msg = f"Missing required coordinate: {coord}"
                raise ValueError(msg)

            actual_dtype = str(self.dataset[coord].dtype)
            if expected_dtype == "object":
                # String coordinates can be stored as object or Unicode string (<U)
                if actual_dtype not in ["object"] and not actual_dtype.startswith("<U"):
                    msg = (
                        f"Coordinate {coord} has wrong dtype: "
                        f"expected string, got {actual_dtype}"
                    )
                    raise ValueError(msg)
            elif expected_dtype not in actual_dtype:
                msg = (
                    f"Coordinate {coord} has wrong dtype: "
                    f"expected {expected_dtype}, got {actual_dtype}"
                )
                raise ValueError(msg)

    def validate_data_variables(self, required_vars: list[str] | None = None) -> None:
        """Validate required data variables exist.

        Parameters
        ----------
        required_vars : list of str, optional
            List of required variables. If None, uses default minimum set.

        Returns
        -------
        None

        Raises
        ------
        ValueError
            If required variables are missing or have incorrect dimensions.

        """
        if required_vars is None:
            # Minimum required for VOD calculation
            required_vars = ["SNR", "Phase"]

        missing_vars = set(required_vars) - set(self.dataset.data_vars)
        if missing_vars:
            msg = f"Missing required data variables: {missing_vars}"
            raise ValueError(msg)

        # Validate all data vars have (epoch, sid) dimensions
        for var in self.dataset.data_vars:
            expected_dims = ("epoch", "sid")
            actual_dims = self.dataset[var].dims
            if actual_dims != expected_dims:
                msg = (
                    f"Data variable {var} has wrong dimensions: "
                    f"expected {expected_dims}, got {actual_dims}"
                )
                raise ValueError(msg)

    def validate_attributes(self) -> None:
        """Validate required global attributes for storage.

        Returns
        -------
        None

        Raises
        ------
        ValueError
            If required attributes (Created, Software, Institution,
            File Hash) are missing.

        """
        attrs_keys = set(self.dataset.attrs.keys())

        # Accept canonical "File Hash" or legacy "RINEX File Hash"
        hash_keys = {"File Hash", "RINEX File Hash"}
        if not hash_keys.intersection(attrs_keys):
            msg = (
                "Missing required attribute: 'File Hash' (or legacy 'RINEX File Hash')"
            )
            raise ValueError(msg)

        required_attrs = {"Created", "Software", "Institution"}
        missing_attrs = required_attrs - attrs_keys
        if missing_attrs:
            msg = f"Missing required attributes: {missing_attrs}"
            raise ValueError(msg)

    def validate_all(self, required_vars: list[str] | None = None) -> None:
        """Run all validations.

        Parameters
        ----------
        required_vars : list of str, optional
            List of required data variables. If None, uses default minimum set.

        Returns
        -------
        None

        Raises
        ------
        ValueError
            If any validation fails.

        """
        self.validate_dimensions()
        self.validate_coordinates()
        self.validate_data_variables(required_vars)
        self.validate_attributes()


class GNSSDataReader(ABC):
    """Abstract base class for all GNSS data format readers.

    All readers must:
    1. Inherit from this class
    2. Implement all abstract methods
    3. Return xarray.Dataset that passes DatasetStructureValidator
    4. Provide file hash for deduplication

    This ensures compatibility with:
    - canvod-vod: VOD calculation
    - canvod-store: MyIcechunkStore storage
    - canvod-grids: Grid projection operations

    Examples
    --------
    >>> class Rnxv3Obs(GNSSDataReader):
    ...     def to_ds(self, **kwargs) -> xr.Dataset:
    ...         # Implementation
    ...         return dataset
    ...
    >>> reader = Rnxv3Obs(fpath="station.24o")
    >>> ds = reader.to_ds()
    >>> reader.validate_output(ds)  # Automatic validation

    Notes
    -----
    This class uses ``ABC`` and defines abstract methods and properties
    for reader implementations.

    """

    # Note: fpath is not @abstractmethod because Pydantic models define it as a field
    # which provides the same interface
    fpath: Path

    @property
    @abstractmethod
    def file_hash(self) -> str:
        """Return SHA256 hash of file for deduplication.

        Used by MyIcechunkStore to avoid duplicate ingestion.
        Must be deterministic and reproducible.

        Returns
        -------
        str
            Short hash (16 chars) or full hash of file content

        """

    @abstractmethod
    def to_ds(
        self,
        keep_rnx_data_vars: list[str] | None = None,
        **kwargs: object,
    ) -> xr.Dataset:
        """Convert data to xarray.Dataset.

        Must return Dataset with structure:
        - Dims: (epoch, sid)
        - Coords: epoch, sid, sv, system, band, code, freq_*
        - Data vars: At minimum SNR, Phase
        - Attrs: Must include "RINEX File Hash"

        Parameters
        ----------
        keep_rnx_data_vars : list of str, optional
            Data variables to include. If None, includes all available.
        **kwargs
            Implementation-specific parameters

        Returns
        -------
        xr.Dataset
            Dataset that passes DatasetStructureValidator.

        """

    @abstractmethod
    def iter_epochs(self) -> Iterator[object]:
        """Iterate over epochs in the file.

        Returns
        -------
        Generator
            Generator yielding Epoch objects.

        Yields
        ------
        Epoch
            Parsed epoch with satellites and observations.

        """

    def to_ds_and_auxiliary(
        self,
        keep_rnx_data_vars: list[str] | None = None,
        **kwargs: object,
    ) -> "tuple[xr.Dataset, dict[str, xr.Dataset]]":
        """Produce the obs dataset and any auxiliary datasets in a single call.

        Default: calls ``to_ds(**kwargs)`` and returns an empty auxiliary dict.
        Readers that produce metadata (e.g. SBF) override this to collect both
        in a single file scan.

        Returns
        -------
        tuple[xr.Dataset, dict[str, xr.Dataset]]
            ``(obs_ds, {"name": aux_ds, ...})``.  Auxiliary dict is empty for
            readers with no extra data (RINEX v2/v3).
        """
        return self.to_ds(keep_rnx_data_vars=keep_rnx_data_vars, **kwargs), {}

    def validate_output(
        self, dataset: xr.Dataset, required_vars: list[str] | None = None
    ) -> None:
        """Validate output Dataset structure.

        Called automatically by to_ds() to ensure compatibility.
        Can be called manually for testing.

        Parameters
        ----------
        dataset : xr.Dataset
            Dataset to validate
        required_vars : list of str, optional
            Required data variables. If None, uses minimum set.

        Returns
        -------
        None

        Raises
        ------
        ValueError
            If Dataset doesn't meet requirements.

        """
        validator = DatasetStructureValidator(dataset=dataset)
        validator.validate_all(required_vars=required_vars)

    @property
    @abstractmethod
    def start_time(self) -> datetime:
        """Return start time of observations.

        Returns
        -------
        datetime
            First observation timestamp in the file.

        """

    @property
    @abstractmethod
    def end_time(self) -> datetime:
        """Return end time of observations.

        Returns
        -------
        datetime
            Last observation timestamp in the file.

        """

    @property
    @abstractmethod
    def systems(self) -> list[str]:
        """Return list of GNSS systems in file.

        Returns
        -------
        list of str
            System identifiers: 'G', 'R', 'E', 'C', 'J', 'S', 'I'

        """

    @property
    @abstractmethod
    def num_epochs(self) -> int:
        """Return number of epochs in file.

        Returns
        -------
        int
            Total number of observation epochs.

        """

    @property
    @abstractmethod
    def num_satellites(self) -> int:
        """Return total number of unique satellites observed.

        Returns
        -------
        int
            Count of unique satellite vehicles across all systems.

        """

    def __repr__(self) -> str:
        """Return the string representation."""
        return (
            f"{self.__class__.__name__}("
            f"file='{self.fpath.name}', "
            f"systems={self.systems}, "
            f"epochs={self.num_epochs})"
        )


class ReaderFactory:
    """Factory for creating appropriate reader based on file format.

    Automatically detects format and instantiates correct reader.

    Examples
    --------
    >>> reader = ReaderFactory.create("station.24o")
    >>> isinstance(reader, Rnxv3Obs)
    True

    >>> reader = ReaderFactory.create("station.10o")
    >>> isinstance(reader, Rnxv2Obs)
    True

    """

    _readers: ClassVar[dict[str, type]] = {}

    @classmethod
    def register(cls, format_name: str, reader_class: type) -> None:
        """Register a reader class for a format.

        Parameters
        ----------
        format_name : str
            Format identifier (e.g., 'rinex_v3', 'rinex_v2')
        reader_class : type
            Reader class (must inherit from GNSSDataReader)

        Returns
        -------
        None

        Raises
        ------
        TypeError
            If reader_class does not inherit from GNSSDataReader.

        """
        if not issubclass(reader_class, GNSSDataReader):
            msg = f"{reader_class} must inherit from GNSSDataReader"
            raise TypeError(msg)
        cls._readers[format_name] = reader_class

    @classmethod
    def create(
        cls,
        fpath: Path | str,
        **kwargs: object,
    ) -> GNSSDataReader:
        """Create appropriate reader for file.

        Parameters
        ----------
        fpath : Path or str
            Path to data file
        **kwargs
            Parameters to pass to reader constructor

        Returns
        -------
        GNSSDataReader
            Instantiated reader.

        Raises
        ------
        ValueError
            If file format cannot be determined.

        """
        fpath = Path(fpath)

        if not fpath.exists():
            msg = f"File not found: {fpath}"
            raise FileNotFoundError(msg)

        # Detect format from file
        format_name = cls._detect_format(fpath)

        if format_name not in cls._readers:
            msg = (
                f"No reader registered for format: {format_name}. "
                f"Available: {list(cls._readers.keys())}"
            )
            raise ValueError(msg)

        reader_class = cls._readers[format_name]
        return reader_class(fpath=fpath, **kwargs)

    @staticmethod
    def _detect_format(fpath: Path) -> str:
        """Detect file format.

        Parameters
        ----------
        fpath : Path
            Path to file

        Returns
        -------
        str
            Format name.

        """
        # Check RINEX version from first line
        with fpath.open() as f:
            first_line = f.readline()

        # RINEX version is in columns 1-9
        try:
            version_str = first_line[:9].strip()
            version = float(version_str)
        except (ValueError, IndexError) as e:
            msg = f"Cannot determine file format: {e}"
            raise ValueError(msg) from e

        rinex_v2_min = 2.0
        rinex_v3_min = 3.0
        rinex_v4_min = 4.0

        if rinex_v3_min <= version < rinex_v4_min:
            return "rinex_v3"
        if rinex_v2_min <= version < rinex_v3_min:
            return "rinex_v2"
        msg = f"Unsupported RINEX version: {version}"
        raise ValueError(msg)

    @classmethod
    def list_formats(cls) -> list[str]:
        """List available formats.

        Returns
        -------
        list of str
            Registered format identifiers.

        """
        return list(cls._readers.keys())


# Backwards compatibility aliases
GNSSReader = GNSSDataReader
RinexReader = GNSSDataReader

__all__ = [
    "DatasetStructureValidator",
    "GNSSDataReader",
    "GNSSReader",
    "ReaderFactory",
    "RinexReader",
]
