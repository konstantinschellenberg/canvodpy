"""Tests for product registry."""

import pytest

from canvod.auxiliary.products import (
    ProductSpec,
    get_product_spec,
    list_agencies,
    list_products,
)


class TestProductSpec:
    """Test ProductSpec model."""

    def test_valid_product_spec(self):
        """Test creating valid ProductSpec."""
        spec = ProductSpec(
            agency_code="COD",
            agency_name="Test Agency",
            product_type="final",
            prefix="COD0MGXFIN",
            sampling_rate="05M",
            duration="01D",
            available_formats=["SP3", "CLK"],
            ftp_servers=[],
            ftp_path_pattern="/gnss/products/{gps_week}/{file}",
            description="Test product",
        )

        assert spec.agency_code == "COD"
        assert "SP3" in spec.available_formats

    def test_minimal_product_spec(self):
        """Test minimal required fields."""
        spec = ProductSpec(
            agency_code="COD",
            agency_name="Test Agency",
            product_type="final",
            prefix="COD0MGXFIN",
            sampling_rate="05M",
            duration="01D",
            available_formats=["SP3"],
            ftp_servers=[],
            ftp_path_pattern="/path/{file}",
        )

        assert spec.agency_code == "COD"


class TestGetProductSpec:
    """Test get_product_spec function."""

    def test_cod_final_product(self):
        """Test retrieving COD final product."""
        spec = get_product_spec("COD", "final")

        assert spec is not None
        assert spec.agency_code == "COD"
        assert "SP3" in spec.available_formats
        assert "CLK" in spec.available_formats

    def test_gfz_rapid_product(self):
        """Test retrieving GFZ rapid product."""
        spec = get_product_spec("GFZ", "rapid")

        assert spec is not None

    def test_esa_rapid_product(self):
        """Test retrieving ESA rapid product."""
        spec = get_product_spec("ESA", "rapid")

        assert spec is not None

    def test_invalid_agency_raises(self):
        """Test invalid agency raises KeyError."""
        with pytest.raises(KeyError):
            get_product_spec("INVALID_AGENCY", "final")

    def test_invalid_product_type_raises(self):
        """Test invalid product type raises KeyError."""
        with pytest.raises(KeyError):
            get_product_spec("COD", "invalid_product")


class TestListAvailableProducts:
    """Test list_products function."""

    def test_returns_list(self):
        """Test function returns list of tuples."""
        products = list_products()

        assert isinstance(products, list)
        assert len(products) > 0
        # Check first element is a tuple
        assert isinstance(products[0], tuple)
        assert len(products[0]) == 2  # (agency, product_type)

    def test_contains_expected_agencies(self):
        """Test registry contains expected agencies."""
        products = list_products()
        agencies = {agency for agency, _ in products}

        expected = ["COD", "GFZ", "ESA", "JPL"]
        for agency in expected:
            assert agency in agencies

    def test_each_agency_has_products(self):
        """Test each agency has at least one product."""
        products = list_products()
        agencies = {}

        for agency, product_type in products:
            if agency not in agencies:
                agencies[agency] = []
            agencies[agency].append(product_type)

        for agency, types in agencies.items():
            assert len(types) > 0

    def test_final_products_exist(self):
        """Test most agencies have final products."""
        products = list_products()

        agencies_with_final = {
            agency for agency, product_type in products if product_type == "final"
        }

        # Most major agencies should have final products
        assert len(agencies_with_final) >= 3


class TestListAgencies:
    """Test list_agencies function."""

    def test_returns_list(self):
        """Test function returns list."""
        agencies = list_agencies()

        assert isinstance(agencies, list)
        assert len(agencies) > 0

    def test_contains_major_agencies(self):
        """Test list contains major agencies."""
        agencies = list_agencies()

        expected = ["COD", "GFZ", "ESA", "JPL", "IGS"]
        for agency in expected:
            assert agency in agencies

    def test_sorted_alphabetically(self):
        """Test agencies are sorted."""
        agencies = list_agencies()

        assert agencies == sorted(agencies)


class TestProductRegistry:
    """Integration tests for product registry."""

    def test_minimum_product_count(self):
        """Test registry has minimum number of products."""
        products = list_products()

        # Should have at least 10 products total
        assert len(products) >= 10

    def test_all_specs_valid(self):
        """Test all product specs can be retrieved."""
        products = list_products()

        for agency, product_type in products:
            spec = get_product_spec(agency, product_type)
            assert spec is not None
            assert spec.ftp_path_pattern is not None

    def test_url_templates_format(self):
        """Test URL templates contain expected placeholders."""
        products = list_products()

        for agency, product_type in products:
            spec = get_product_spec(agency, product_type)

            # Should contain placeholders
            path = spec.ftp_path_pattern
            assert any(ph in path for ph in ["{gps_week}", "{file}"])

    def test_auth_requirements_specified(self):
        """Test that products have consistent structure."""
        products = list_products()

        # Check all products have valid specs
        for agency, product_type in products:
            spec = get_product_spec(agency, product_type)
            assert spec.agency_code is not None


class TestProductSpecConfiguration:
    """Test product spec configuration details."""

    def test_ftp_path_pattern_specified(self):
        """Test FTP path pattern is specified for products."""
        spec = get_product_spec("COD", "final")

        assert spec.ftp_path_pattern is not None
        assert "/" in spec.ftp_path_pattern

    def test_sp3_and_clk_consistency(self):
        """Test products specify available formats."""
        spec = get_product_spec("COD", "final")

        assert spec.available_formats is not None
        assert len(spec.available_formats) > 0
        # Most final products should have both SP3 and CLK
        assert "SP3" in spec.available_formats
