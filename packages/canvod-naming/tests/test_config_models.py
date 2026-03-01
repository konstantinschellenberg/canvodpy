"""Tests for canvod.naming.config_models."""

import pytest
from pydantic import ValidationError

from canvod.naming.config_models import (
    DirectoryLayout,
    ReceiverNamingConfig,
    SiteNamingConfig,
)


class TestSiteNamingConfig:
    def test_minimal(self):
        cfg = SiteNamingConfig(site_id="ROS", agency="TUW")
        assert cfg.site_id == "ROS"
        assert cfg.agency == "TUW"
        assert cfg.default_sampling == "05S"
        assert cfg.default_period == "01D"
        assert cfg.default_content == "AA"

    def test_custom_defaults(self):
        cfg = SiteNamingConfig(
            site_id="HAI",
            agency="GFZ",
            default_sampling="01S",
            default_period="15M",
            default_content="MO",
        )
        assert cfg.default_sampling == "01S"
        assert cfg.default_period == "15M"
        assert cfg.default_content == "MO"

    def test_invalid_site_id(self):
        with pytest.raises(ValidationError):
            SiteNamingConfig(site_id="ROSX", agency="TUW")

    def test_invalid_agency(self):
        with pytest.raises(ValidationError):
            SiteNamingConfig(site_id="ROS", agency="TU")

    def test_invalid_sampling(self):
        with pytest.raises(ValidationError):
            SiteNamingConfig(site_id="ROS", agency="TUW", default_sampling="XY")

    def test_from_dict(self):
        d = {"site_id": "ROS", "agency": "TUW", "default_sampling": "05S"}
        cfg = SiteNamingConfig.model_validate(d)
        assert cfg.site_id == "ROS"


class TestReceiverNamingConfig:
    def test_minimal(self):
        cfg = ReceiverNamingConfig(receiver_number=1)
        assert cfg.receiver_number == 1
        assert cfg.source_pattern == "auto"
        assert cfg.directory_layout == DirectoryLayout.YYDDD_SUBDIRS
        assert cfg.agency is None
        assert cfg.sampling is None

    def test_full(self):
        cfg = ReceiverNamingConfig(
            receiver_number=2,
            source_pattern="septentrio_sbf",
            directory_layout=DirectoryLayout.FLAT,
            agency="GFZ",
            sampling="01S",
            period="15M",
            content="MO",
        )
        assert cfg.receiver_number == 2
        assert cfg.source_pattern == "septentrio_sbf"
        assert cfg.directory_layout == DirectoryLayout.FLAT

    def test_receiver_number_bounds(self):
        with pytest.raises(ValidationError):
            ReceiverNamingConfig(receiver_number=0)
        with pytest.raises(ValidationError):
            ReceiverNamingConfig(receiver_number=100)

    def test_metadata_none_by_default(self):
        cfg = ReceiverNamingConfig(receiver_number=1)
        assert cfg.metadata is None

    def test_metadata_with_values(self):
        cfg = ReceiverNamingConfig(
            receiver_number=1,
            metadata={
                "site_url": "https://example.com",
                "antenna_height": 1.5,
                "species": "Fagus sylvatica",
                "is_permanent": True,
            },
        )
        assert cfg.metadata["site_url"] == "https://example.com"
        assert cfg.metadata["antenna_height"] == 1.5
        assert cfg.metadata["species"] == "Fagus sylvatica"
        assert cfg.metadata["is_permanent"] is True

    def test_from_dict(self):
        d = {
            "receiver_number": 1,
            "source_pattern": "auto",
            "directory_layout": "yyddd_subdirs",
        }
        cfg = ReceiverNamingConfig.model_validate(d)
        assert cfg.directory_layout == DirectoryLayout.YYDDD_SUBDIRS

    def test_from_dict_with_metadata(self):
        d = {
            "receiver_number": 1,
            "metadata": {"species": "Picea abies", "antenna_height": 2.0},
        }
        cfg = ReceiverNamingConfig.model_validate(d)
        assert cfg.metadata["species"] == "Picea abies"


class TestDirectoryLayout:
    def test_values(self):
        assert DirectoryLayout.YYDDD_SUBDIRS.value == "yyddd_subdirs"
        assert DirectoryLayout.YYYYDDD_SUBDIRS.value == "yyyyddd_subdirs"
        assert DirectoryLayout.FLAT.value == "flat"

    def test_from_string(self):
        assert DirectoryLayout("flat") == DirectoryLayout.FLAT
