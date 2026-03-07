"""Tests for NamingRecipe — user-defined filename mapping."""

from __future__ import annotations

import pytest

from canvod.virtualiconvname.recipe import KNOWN_FIELDS, NamingRecipe

# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture
def septentrio_v2_recipe() -> NamingRecipe:
    """Recipe for Septentrio RINEX v2 with minute: rref001a15.25o"""
    return NamingRecipe(
        name="rosalia_reference",
        description="Rosalia reference receiver",
        site="ROS",
        agency="TUW",
        receiver_number=1,
        receiver_type="reference",
        sampling="05S",
        period="15M",
        content="AA",
        file_type="rnx",
        glob="*.??o",
        fields=[
            {"skip": 4},
            {"doy": 3},
            {"hour_letter": 1},
            {"minute": 2},
            {"skip": 1},
            {"yy": 2},
            {"skip": 1},
        ],
    )


@pytest.fixture
def exotic_recipe() -> NamingRecipe:
    """Recipe for exotic filenames: STATION_2025_042_00_15.rinex"""
    return NamingRecipe(
        name="exotic_station",
        site="EXO",
        agency="TST",
        receiver_number=1,
        receiver_type="canopy",
        sampling="30S",
        period="01H",
        content="S1",
        file_type="rnx",
        glob="*.rinex",
        fields=[
            {"skip": 8},
            {"year": 4},
            {"skip": 1},
            {"doy": 3},
            {"skip": 1},
            {"hour": 2},
            {"skip": 1},
            {"minute": 2},
            {"skip": 6},
        ],
    )


@pytest.fixture
def daily_recipe() -> NamingRecipe:
    """Recipe for daily files with no hour/minute: data_2025_042.obs"""
    return NamingRecipe(
        name="daily_only",
        site="DAY",
        agency="TST",
        receiver_number=1,
        receiver_type="reference",
        sampling="01S",
        period="01D",
        content="AA",
        file_type="rnx",
        glob="*.obs",
        fields=[
            {"skip": 5},
            {"year": 4},
            {"skip": 1},
            {"doy": 3},
            {"skip": 4},
        ],
    )


@pytest.fixture
def month_day_recipe() -> NamingRecipe:
    """Recipe using month+day instead of DOY: site_20250301_1200.dat"""
    return NamingRecipe(
        name="month_day",
        site="MDS",
        agency="TST",
        receiver_number=1,
        receiver_type="canopy",
        sampling="05S",
        period="01H",
        content="AA",
        file_type="rnx",
        glob="*.dat",
        fields=[
            {"skip": 5},
            {"year": 4},
            {"month": 2},
            {"day": 2},
            {"skip": 1},
            {"hour": 2},
            {"minute": 2},
            {"skip": 4},
        ],
    )


# ---------------------------------------------------------------------------
# Tests: Parsing
# ---------------------------------------------------------------------------


class TestParseFilename:
    def test_septentrio_v2(self, septentrio_v2_recipe):
        result = septentrio_v2_recipe.parse_filename("rref001a15.25o")
        assert result == {"doy": 1, "hour_letter": "a", "minute": 15, "yy": 25}

    def test_septentrio_v2_different_time(self, septentrio_v2_recipe):
        result = septentrio_v2_recipe.parse_filename("rref042x45.25o")
        assert result["doy"] == 42
        assert result["hour_letter"] == "x"
        assert result["minute"] == 45

    def test_exotic(self, exotic_recipe):
        result = exotic_recipe.parse_filename("STATION_2025_042_00_15.rinex")
        assert result == {"year": 2025, "doy": 42, "hour": 0, "minute": 15}

    def test_month_day(self, month_day_recipe):
        result = month_day_recipe.parse_filename("site_20250301_1200.dat")
        assert result == {
            "year": 2025,
            "month": 3,
            "day": 1,
            "hour": 12,
            "minute": 0,
        }

    def test_filename_too_short(self, septentrio_v2_recipe):
        with pytest.raises(ValueError, match="too short"):
            septentrio_v2_recipe.parse_filename("rref")

    def test_non_integer_field(self, septentrio_v2_recipe):
        with pytest.raises(ValueError, match="Cannot parse"):
            septentrio_v2_recipe.parse_filename("rrefABCa15.25o")


# ---------------------------------------------------------------------------
# Tests: Virtual file mapping
# ---------------------------------------------------------------------------


class TestToVirtualFile:
    def test_septentrio_v2(self, septentrio_v2_recipe, tmp_path):
        f = tmp_path / "rref001a15.25o"
        f.touch()
        vf = septentrio_v2_recipe.to_virtual_file(f)
        cn = vf.conventional_name
        assert cn.site == "ROS"
        assert cn.receiver_type.value == "R"
        assert cn.year == 2025
        assert cn.doy == 1
        assert cn.hour == 0  # hour_letter 'a' = hour 0
        assert cn.minute == 15
        assert cn.period == "15M"
        assert cn.sampling == "05S"

    def test_exotic(self, exotic_recipe, tmp_path):
        f = tmp_path / "STATION_2025_042_00_15.rinex"
        f.touch()
        vf = exotic_recipe.to_virtual_file(f)
        cn = vf.conventional_name
        assert cn.site == "EXO"
        assert cn.year == 2025
        assert cn.doy == 42
        assert cn.hour == 0
        assert cn.minute == 15
        assert cn.period == "01H"

    def test_daily_no_hour(self, daily_recipe, tmp_path):
        f = tmp_path / "data_2025_042.obs"
        f.touch()
        vf = daily_recipe.to_virtual_file(f)
        cn = vf.conventional_name
        assert cn.hour == 0
        assert cn.minute == 0
        assert cn.period == "01D"

    def test_month_day_to_doy(self, month_day_recipe, tmp_path):
        f = tmp_path / "site_20250301_1200.dat"
        f.touch()
        vf = month_day_recipe.to_virtual_file(f)
        cn = vf.conventional_name
        # March 1 = DOY 60
        assert cn.doy == 60
        assert cn.hour == 12
        assert cn.minute == 0

    def test_yy_resolves_to_2025(self, septentrio_v2_recipe, tmp_path):
        f = tmp_path / "rref001a15.25o"
        f.touch()
        vf = septentrio_v2_recipe.to_virtual_file(f)
        assert vf.conventional_name.year == 2025

    def test_canonical_name_format(self, septentrio_v2_recipe, tmp_path):
        f = tmp_path / "rref001a15.25o"
        f.touch()
        vf = septentrio_v2_recipe.to_virtual_file(f)
        assert vf.canonical_str == "ROSR01TUW_R_20250010015_15M_05S_AA.rnx"


# ---------------------------------------------------------------------------
# Tests: matches()
# ---------------------------------------------------------------------------


class TestMatches:
    def test_matching_filename(self, septentrio_v2_recipe):
        assert septentrio_v2_recipe.matches("rref001a15.25o") is True

    def test_non_matching_filename(self, septentrio_v2_recipe):
        assert septentrio_v2_recipe.matches("short.o") is False


# ---------------------------------------------------------------------------
# Tests: Serialization
# ---------------------------------------------------------------------------


class TestSerialization:
    def test_yaml_roundtrip(self, septentrio_v2_recipe):
        yaml_str = septentrio_v2_recipe.to_yaml()
        loaded = NamingRecipe.from_yaml(yaml_str)
        assert loaded == septentrio_v2_recipe

    def test_json_roundtrip(self, septentrio_v2_recipe):
        json_str = septentrio_v2_recipe.model_dump_json()
        loaded = NamingRecipe.model_validate_json(json_str)
        assert loaded == septentrio_v2_recipe

    def test_save_load(self, septentrio_v2_recipe, tmp_path):
        path = tmp_path / "recipe.yaml"
        septentrio_v2_recipe.save(path)
        loaded = NamingRecipe.load(path)
        assert loaded == septentrio_v2_recipe

    def test_yaml_is_human_readable(self, septentrio_v2_recipe):
        yaml_str = septentrio_v2_recipe.to_yaml()
        assert "rosalia_reference" in yaml_str
        assert "ROS" in yaml_str
        assert "fields:" in yaml_str


# ---------------------------------------------------------------------------
# Tests: Validation
# ---------------------------------------------------------------------------


class TestValidation:
    def test_unknown_field_name(self):
        with pytest.raises(ValueError, match="unknown field 'garbage'"):
            NamingRecipe(
                name="bad",
                site="TST",
                agency="TST",
                receiver_number=1,
                glob="*",
                fields=[{"garbage": 3}],
            )

    def test_multi_key_entry(self):
        with pytest.raises(ValueError, match="single key-value pair"):
            NamingRecipe(
                name="bad",
                site="TST",
                agency="TST",
                receiver_number=1,
                glob="*",
                fields=[{"doy": 3, "year": 4}],
            )

    def test_zero_width(self):
        with pytest.raises(ValueError, match="positive integer"):
            NamingRecipe(
                name="bad",
                site="TST",
                agency="TST",
                receiver_number=1,
                glob="*",
                fields=[{"doy": 0}],
            )

    def test_known_fields_documented(self):
        """Ensure KNOWN_FIELDS matches what we expect."""
        expected = {
            "year",
            "yy",
            "doy",
            "month",
            "day",
            "hour",
            "minute",
            "hour_letter",
            "skip",
        }
        assert KNOWN_FIELDS == expected

    def test_no_year_or_doy_raises(self, tmp_path):
        recipe = NamingRecipe(
            name="missing_year",
            site="TST",
            agency="TST",
            receiver_number=1,
            glob="*",
            fields=[{"skip": 5}],
        )
        f = tmp_path / "hello"
        f.touch()
        with pytest.raises(ValueError, match="no 'year' or 'yy'"):
            recipe.to_virtual_file(f)
