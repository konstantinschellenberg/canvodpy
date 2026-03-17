"""Meta tests for canvod-readers package."""


class TestPackageStructure:
    """Test package structure and imports."""

    def test_package_imports(self):
        """Test that package can be imported."""
        import canvod.readers

        assert canvod.readers is not None

    def test_main_exports(self):
        """Test main exports are available."""
        from canvod.readers import GNSSDataReader, Rnxv3Obs, validate_dataset

        assert GNSSDataReader is not None
        assert Rnxv3Obs is not None
        assert validate_dataset is not None

    def test_base_classes(self):
        """Test base classes can be imported."""
        from canvod.readers.base import GNSSDataReader, validate_dataset

        assert GNSSDataReader is not None
        assert validate_dataset is not None

    def test_shared_modules(self):
        """Test shared modules can be imported."""
        from canvod.readers.gnss_specs import (
            constants,
            exceptions,
            metadata,
            models,
            signals,
            utils,
        )

        assert constants is not None
        assert exceptions is not None
        assert metadata is not None
        assert models is not None
        assert signals is not None
        assert utils is not None

    def test_rinex_module(self):
        """Test RINEX module can be imported."""
        from canvod.readers.rinex import Rnxv3Obs

        assert Rnxv3Obs is not None

    def test_rinex_v3_04_classes(self):
        """Test RINEX v3.04 classes can be imported."""
        from canvod.readers.rinex.v3_04 import Rnxv3Header, Rnxv3Obs

        assert Rnxv3Header is not None
        assert Rnxv3Obs is not None


class TestPackageMetadata:
    """Test package metadata."""

    def test_version_exists(self):
        """Test package has version."""
        import canvod.readers

        assert hasattr(canvod.readers, "__version__")
        assert canvod.readers.__version__ is not None

    def test_all_exports(self):
        """Test __all__ is defined."""
        import canvod.readers

        assert hasattr(canvod.readers, "__all__")
        assert isinstance(canvod.readers.__all__, list)
        assert len(canvod.readers.__all__) > 0
