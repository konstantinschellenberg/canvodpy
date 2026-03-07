"""Pydantic models for naming configuration in sites.yaml.

These models validate the ``naming:`` sections at site and receiver level.
The ``canvod-utils`` package stores these as opaque ``dict | None`` fields;
this package validates them when constructing a ``FilenameMapper``.
"""

from __future__ import annotations

from enum import StrEnum

from pydantic import BaseModel, Field

from .convention import AgencyId, ContentCode, Duration, SiteId


class DirectoryLayout(StrEnum):
    """How receiver data files are organised into subdirectories."""

    YYDDD_SUBDIRS = "yyddd_subdirs"  # 25001/, 25002/
    YYYYDDD_SUBDIRS = "yyyyddd_subdirs"  # 2025001/
    FLAT = "flat"  # all files in one directory


class SiteNamingConfig(BaseModel):
    """Site-level naming defaults (``sites.<name>.naming`` in YAML)."""

    site_id: SiteId
    agency: AgencyId
    default_sampling: Duration = "05S"
    default_period: Duration = "01D"
    default_content: ContentCode = "AA"


class ReceiverNamingConfig(BaseModel):
    """Receiver-level naming overrides (``sites.<name>.receivers.<rx>.naming``)."""

    receiver_number: int = Field(ge=1, le=99)
    source_pattern: str = "auto"
    directory_layout: DirectoryLayout = DirectoryLayout.YYDDD_SUBDIRS
    agency: AgencyId | None = None
    sampling: Duration | None = None
    period: Duration | None = None
    content: ContentCode | None = None
