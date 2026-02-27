"""Configuration-based product registry with Pydantic validation."""

import tomllib
from pathlib import Path

from pydantic import BaseModel, ConfigDict, Field, field_validator


class FtpServerConfig(BaseModel):
    """FTP server configuration with validation.

    Notes
    -----
    This is a Pydantic `BaseModel`.
    """

    model_config = ConfigDict(extra="forbid")

    url: str = Field(pattern=r"^ftps?://")
    priority: int = Field(ge=1, le=100)
    description: str = ""
    requires_auth: bool = False

    @field_validator("url")
    @classmethod
    def validate_url_scheme(cls, v: str) -> str:
        """Validate URL scheme is ftp or ftps."""
        if not (v.startswith("ftp://") or v.startswith("ftps://")):
            raise ValueError("URL must start with ftp:// or ftps://")
        return v

    # TODO: Check


class ProductSpec(BaseModel):
    """
    Product specification with structural validation.

    Validates data structure only (fast).
    FTP availability validated during download (lazy, fail-fast).

    Notes
    -----
    This is a Pydantic `BaseModel`.
    """

    model_config = ConfigDict(extra="forbid")

    agency_code: str = Field(
        min_length=3,
        max_length=3,
        pattern=r"^[A-Z]{3}$",
    )
    agency_name: str
    product_type: str = Field(
        pattern=r"^(final|rapid|ultrarapid|real-time|near-real-time)$",
    )
    prefix: str = Field(
        min_length=9,
        max_length=10,
        pattern=r"^[A-Z0-9]+$",
    )
    sampling_rate: str = Field(pattern=r"^\d{2}[SMHD]$")
    duration: str = Field(pattern=r"^\d{2}[DH]$")
    available_formats: list[str]
    ftp_servers: list[FtpServerConfig]
    ftp_path_pattern: str
    description: str = ""

    @field_validator("prefix")
    @classmethod
    def validate_prefix_matches_agency(cls, v: str, info) -> str:
        """Validate prefix starts with agency code."""
        if "agency_code" in info.data:
            agency = info.data["agency_code"]
            if not v.startswith(agency):
                raise ValueError(f"Prefix must start with agency code {agency}")
        return v


class ProductRegistry:
    """Product registry loaded from TOML configuration.

    Allows users to:
    - Add custom products without editing code
    - Configure custom FTP servers
    - Override default products
    - Version control their configurations

    Parameters
    ----------
    config_path : Path, optional
        Path to products.toml (defaults to package location).
    """

    def __init__(self, config_path: Path | None = None):
        """Initialize registry from config file."""
        if config_path is None:
            # Default: package-provided config
            config_path = Path(__file__).parent / "products.toml"

        self.config_path = Path(config_path)
        self._products: dict[tuple[str, str], ProductSpec] = {}
        self._agencies: dict[str, str] = {}

        # Load configuration
        self._load_config()

    def _load_config(self) -> None:
        """Load products from TOML file with Pydantic validation."""
        if not self.config_path.exists():
            raise FileNotFoundError(
                f"Config not found: {self.config_path}\n"
                f"Expected location: {Path(__file__).parent / 'products.toml'}"
            )

        with open(self.config_path, "rb") as f:
            config = tomllib.load(f)

        # Load agency metadata
        self._agencies = config.get("agencies", {})

        # Load products with Pydantic validation
        for product_data in config.get("products", []):
            # Build ProductSpec with Pydantic validation
            spec = ProductSpec(
                agency_code=product_data["agency"],
                agency_name=self._agencies.get(
                    product_data["agency"], f"{product_data['agency']} Analysis Center"
                ),
                product_type=product_data["type"],
                prefix=product_data["prefix"],
                sampling_rate=product_data["sampling"],
                duration=product_data["duration"],
                available_formats=product_data.get("formats", ["SP3", "CLK"]),
                ftp_servers=[
                    FtpServerConfig(**server)
                    for server in product_data.get("ftp_servers", [])
                ],
                ftp_path_pattern=product_data["ftp_path"],
                description=product_data.get("description", ""),
            )

            # Store by (agency, type) key
            key = (spec.agency_code, spec.product_type)
            self._products[key] = spec

    def get_product_spec(self, agency: str, product_type: str) -> ProductSpec:
        """Get product specification.

        Parameters
        ----------
        agency : str
            Agency code (e.g., "COD", "GFZ").
        product_type : str
            Product type (e.g., "final", "rapid", "ultrarapid").

        Returns
        -------
        ProductSpec
            Product specification.

        Raises
        ------
        KeyError
            If product not found.
        """
        key = (agency.upper(), product_type.lower())

        if key not in self._products:
            available = [f"{ag}/{pt}" for ag, pt in self.list_products()]
            raise KeyError(
                f"Product not found: {agency}/{product_type}\n"
                f"Available products ({len(available)}):\n"
                + "\n".join(f"  - {p}" for p in sorted(available)[:10])
                + (
                    f"\n  ... and {len(available) - 10} more"
                    if len(available) > 10
                    else ""
                )
                + f"\n\nTo add this product, edit: {self.config_path}"
            )

        return self._products[key]

    def list_agencies(self) -> list[str]:
        """List all registered agencies."""
        return sorted(set(agency for agency, _ in self._products.keys()))

    def list_products(self) -> list[tuple[str, str]]:
        """List all registered (agency, product_type) combinations."""
        return sorted(self._products.keys())

    def get_products_for_agency(self, agency: str) -> list[str]:
        """Get all product types for an agency."""
        agency_upper = agency.upper()
        return sorted(
            product_type
            for (ag, product_type) in self._products.keys()
            if ag == agency_upper
        )

    def get_agency_name(self, agency: str) -> str:
        """Get full agency name."""
        return self._agencies.get(agency.upper(), f"{agency.upper()} Analysis Center")


# Global registry instance (lazy-loaded)
_registry: ProductRegistry | None = None


def get_registry(config_path: Path | None = None) -> ProductRegistry:
    """Get global registry instance.

    Parameters
    ----------
    config_path : Path, optional
        Optional custom config path.

    Returns
    -------
    ProductRegistry
        Global registry instance.
    """
    global _registry

    if _registry is None or config_path is not None:
        _registry = ProductRegistry(config_path)

    return _registry


# Convenience functions
def get_product_spec(agency: str, product_type: str) -> ProductSpec:
    """Get product spec from global registry."""
    return get_registry().get_product_spec(agency, product_type)


def list_agencies() -> list[str]:
    """List agencies from global registry."""
    return get_registry().list_agencies()


def list_products() -> list[tuple[str, str]]:
    """List all products from global registry."""
    return get_registry().list_products()


def get_products_for_agency(agency: str) -> list[str]:
    """Get products for agency from global registry."""
    return get_registry().get_products_for_agency(agency)


def get_agency_name(agency: str) -> str:
    """Get full agency name from global registry."""
    return get_registry().get_agency_name(agency)
