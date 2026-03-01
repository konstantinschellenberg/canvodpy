"""canvod-naming: Filename convention, mapping engine, and catalog for canVODpy."""

__version__ = "0.1.0"

from .catalog import FilenameCatalog
from .config_models import DirectoryLayout, ReceiverNamingConfig, SiteNamingConfig
from .convention import (
    AgencyId,
    CanVODFilename,
    ContentCode,
    Duration,
    FileType,
    ReceiverType,
    SiteId,
)
from .mapping import FilenameMapper, VirtualFile
from .patterns import BUILTIN_PATTERNS, SourcePattern, match_pattern

__all__ = [
    "BUILTIN_PATTERNS",
    "AgencyId",
    "CanVODFilename",
    "ContentCode",
    "DirectoryLayout",
    "Duration",
    "FileType",
    "FilenameCatalog",
    "FilenameMapper",
    "ReceiverNamingConfig",
    "ReceiverType",
    "SiteId",
    "SiteNamingConfig",
    "SourcePattern",
    "VirtualFile",
    "match_pattern",
]
