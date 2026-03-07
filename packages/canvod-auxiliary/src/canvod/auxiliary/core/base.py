"""Abstract base class for auxiliary GNSS files."""

import datetime
from abc import ABC, abstractmethod
from pathlib import Path
from typing import Any

import xarray as xr
from canvod.utils.tools import YYYYDOY
from pydantic import ConfigDict, Field
from pydantic.dataclasses import dataclass

from canvod.auxiliary.core.downloader import FileDownloader, FtpDownloader
from canvod.auxiliary.interpolation import Interpolator


@dataclass(config=ConfigDict(arbitrary_types_allowed=True))
class AuxFile(ABC):
    """Abstract base class for GNSS auxiliary files (SP3, CLK, IONEX, etc.).

    This class provides two ways to create instances:
    1. from_datetime_date(): Create from a datetime.date object and metadata
    2. from_file(): Create directly from an existing file path

    The class handles both newly downloaded files and existing local files,
    maintaining consistent behavior regardless of how the instance is created.

    FTP Server Configuration:
    ------------------------
    - user_email: Optional email for NASA CDDIS authentication
      - If None: Uses ESA FTP server exclusively (no authentication required)
      - If provided: Enables NASA CDDIS as fallback server (requires registration)
      - To enable CDDIS, set nasa_earthdata_acc_mail in config/processing.yaml

    Notes
    -----
    This is a Pydantic dataclass with `arbitrary_types_allowed=True`, and
    it uses `ABC` to define required subclass hooks.
    """

    date: str
    agency: str
    product_type: str
    ftp_server: str
    local_dir: Path
    file_type: list[str] = Field(default_factory=list)
    fpath: Path | None = None
    user_email: str | None = None
    downloader: FileDownloader | None = None
    _data: xr.Dataset | None = Field(default=None, init=False)

    def __post_init__(self) -> None:
        """Initialize after dataclass creation.

        Sets up paths, downloader, and verifies local file existence.
        """
        if not self.file_type:
            self.file_type = ["unknown"]
        self.local_dir = Path(self.local_dir)
        self.local_dir.mkdir(parents=True, exist_ok=True)

        if self.downloader is None:
            self.downloader = FtpDownloader(user_email=self.user_email)

        self.fpath = self.check_file_exists()

    @classmethod
    def from_datetime_date(
        cls,
        date: datetime.date,
        agency: str,
        product_type: str,
        ftp_server: str,
        local_dir: Path,
        **kwargs: Any,
    ) -> "AuxFile":
        """Create an AuxFile instance from a datetime.date.

        Parameters
        ----------
        date : datetime.date
            Date for the desired auxiliary file.
        agency : str
            Agency providing the data (e.g., "COD", "IGS").
        product_type : str
            Product type ("final", "rapid", "ultrarapid").
        ftp_server : str
            Base URL for file downloads.
        local_dir : Path
            Directory for storing files locally.
        **kwargs : Any
            Extra keyword arguments for subclass construction.

        Returns
        -------
        AuxFile
            A new instance of the AuxFile subclass.
        """
        yyyydoy = YYYYDOY.from_date(date=date).to_str()
        return cls(
            date=yyyydoy,
            agency=agency,
            product_type=product_type,
            ftp_server=ftp_server,
            local_dir=local_dir,
            **kwargs,
        )

    @classmethod
    def from_file(cls, fpath: Path, **kwargs: Any) -> "AuxFile":
        """Create an AuxFile instance from an existing file path.

        Parameters
        ----------
        fpath : Path
            Path to the existing GNSS file.
        **kwargs : Any
            Extra keyword arguments for subclass construction.

        Returns
        -------
        AuxFile
            A new instance of the AuxFile subclass.

        Raises
        ------
        FileNotFoundError
            If the specified file does not exist.
        """
        if not fpath.exists():
            raise FileNotFoundError(f"File not found: {fpath}")

        fname = fpath.name
        agency = fname[0:3]
        yyyydoy = fname.split("_")[1][0:7]

        return cls(
            date=yyyydoy,
            agency=agency,
            product_type="final",
            ftp_server="N/A",
            local_dir=fpath.parent,
            fpath=fpath,
            **kwargs,
        )

    def download_file(
        self,
        url: str,
        destination: Path,
        file_info: dict | None = None,
    ) -> Path:
        """Download a file using the configured downloader.

        Parameters
        ----------
        url : str
            Download URL.
        destination : Path
            Local file destination.
        file_info : dict, optional
            Extra info passed to the downloader.

        Returns
        -------
        Path
            Path to the downloaded file.
        """
        if self.downloader is None:
            raise RuntimeError("No downloader is configured")
        return self.downloader.download(url, destination, file_info)

    @abstractmethod
    def read_file(self) -> xr.Dataset:
        """Read and parse the auxiliary file.

        Returns
        -------
        xr.Dataset
            Parsed dataset representation of the file.
        """
        pass

    @abstractmethod
    def get_interpolation_strategy(self) -> Interpolator:
        """Get the interpolation strategy for this file type."""
        pass

    @property
    def data(self) -> xr.Dataset:
        """Access the file's data, loading it if necessary."""
        if self._data is None:
            self._data = self.read_file()
            strategy = self.get_interpolation_strategy()
            self._data.attrs["interpolator_config"] = strategy.to_attrs()
        return self._data

    def check_file_exists(self) -> Path:
        """Verify file exists locally or download it if needed.

        Returns
        -------
        Path
            Local file path.
        """
        filename = self.generate_filename_based_on_type()
        file_path = self.local_dir / filename
        if not file_path.exists():
            print(f"File {file_path} does not exist. Downloading...")
            self.download_aux_file()
        else:
            print(f"File {file_path} exists.")
        return file_path

    @abstractmethod
    def generate_filename_based_on_type(self) -> Path:
        """Generate the appropriate filename for this type of auxiliary file."""
        pass

    @abstractmethod
    def download_aux_file(self) -> None:
        """Download the auxiliary file from the specified FTP server."""
        pass
