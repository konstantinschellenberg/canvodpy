"""NMEA-specific exceptions.

Exception Hierarchy
-------------------
NmeaError (defined in gnss_specs.exceptions)
    ├── NmeaChecksumError
    ├── NmeaMissingSentenceError
    └── NmeaInvalidSentenceError
"""

from canvod.readers.gnss_specs.exceptions import NmeaError


class NmeaChecksumError(NmeaError):
    """Raised when NMEA sentence checksum validation fails.

    Parameters
    ----------
    sentence : str
        The raw NMEA sentence with the bad checksum.
    expected : str
        Expected checksum value.
    actual : str
        Computed checksum value.

    """

    def __init__(self, sentence: str, expected: str, actual: str) -> None:
        """Initialize with sentence and checksum details."""
        super().__init__(
            f"Checksum mismatch for '{sentence}': expected {expected}, got {actual}"
        )


class NmeaMissingSentenceError(NmeaError):
    """Raised when required NMEA sentences are missing from the file.

    Parameters
    ----------
    message : str
        Description of which sentences are missing.

    """

    def __init__(self, message: str) -> None:
        """Initialize with a descriptive message."""
        super().__init__(message)


class NmeaInvalidSentenceError(NmeaError):
    """Raised when an NMEA sentence is malformed or cannot be parsed.

    Parameters
    ----------
    message : str
        Description of the parsing failure.

    """

    def __init__(self, message: str) -> None:
        """Initialize with a descriptive message."""
        super().__init__(message)
