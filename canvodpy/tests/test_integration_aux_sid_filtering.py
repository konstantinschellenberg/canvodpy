"""Integration test: aux data filtering matches RINEX SID filtering.

Verifies that the preprocessing pipeline correctly applies SID filtering
and adds spherical coordinates (phi, theta, r) to the output dataset.

Requires:
- config/sites.yaml with a "rosalia" site
- RINEX files in the site's parsed directory
- Pre-built aux Hermite Zarr store
"""

import pytest

from canvod.utils.config import load_config

# ---------------------------------------------------------------------------
# Skip conditions
# ---------------------------------------------------------------------------

try:
    config = load_config()
    rosalia = config.sites.sites.get("rosalia")
except Exception:
    config = None
    rosalia = None


def _has_rinex_files():
    """Check if RINEX files are available for the test."""
    if rosalia is None:
        return False
    parsed_dir = rosalia.get_base_path() / "02_Parsed_RINEX" / "reference_01"
    return parsed_dir.exists() and any(parsed_dir.glob("*.rnx"))


def _has_aux_data():
    """Check if aux Hermite Zarr is available."""
    if rosalia is None:
        return False
    return (rosalia.get_base_path() / "aux_data_hermite.zarr").exists()


pytestmark = pytest.mark.integration

SKIP_NO_CONFIG = pytest.mark.skipif(rosalia is None, reason="No rosalia site in config")
SKIP_NO_DATA = pytest.mark.skipif(
    not _has_rinex_files() or not _has_aux_data(),
    reason="Requires RINEX files and aux Hermite Zarr on disk",
)


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------


@SKIP_NO_CONFIG
@SKIP_NO_DATA
def test_preprocess_adds_spherical_coords():
    """Preprocessing should add phi, theta, r to the output dataset."""
    from canvodpy.orchestrator.processor import preprocess_with_hermite_aux

    from canvod.auxiliary.position import ECEFPosition

    ref_cfg = rosalia.receivers["reference_01"]
    parsed_dir = rosalia.get_base_path() / "02_Parsed_RINEX" / "reference_01"
    rnx_file = sorted(parsed_dir.glob("*.rnx"))[0]
    aux_zarr = rosalia.get_base_path() / "aux_data_hermite.zarr"

    # ECEFPosition must be provided externally (not on ReceiverConfig)
    # Use a dummy position for structural validation
    receiver_pos = ECEFPosition(x=-1224452.587, y=-2689216.073, z=5633638.285)

    _, ds = preprocess_with_hermite_aux(
        rnx_file=rnx_file,
        keep_vars=["SNR"],
        aux_zarr_path=aux_zarr,
        receiver_position=receiver_pos,
        receiver_type="reference",
    )

    assert "phi" in ds.data_vars
    assert "theta" in ds.data_vars
    assert "r" in ds.data_vars
    assert ds.sizes["epoch"] > 0
    assert ds.sizes["sid"] > 0


@SKIP_NO_CONFIG
@SKIP_NO_DATA
def test_sid_filtering_reduces_dataset():
    """Passing keep_sids should reduce the SID dimension."""
    from canvodpy.orchestrator.processor import preprocess_with_hermite_aux

    from canvod.auxiliary.position import ECEFPosition

    parsed_dir = rosalia.get_base_path() / "02_Parsed_RINEX" / "reference_01"
    rnx_file = sorted(parsed_dir.glob("*.rnx"))[0]
    aux_zarr = rosalia.get_base_path() / "aux_data_hermite.zarr"
    receiver_pos = ECEFPosition(x=-1224452.587, y=-2689216.073, z=5633638.285)

    # Process with a small SID subset
    keep_sids = ["G01|L1|C", "G02|L1|C", "E01|E1|C"]

    _, ds = preprocess_with_hermite_aux(
        rnx_file=rnx_file,
        keep_vars=["SNR"],
        aux_zarr_path=aux_zarr,
        receiver_position=receiver_pos,
        receiver_type="reference",
        keep_sids=keep_sids,
    )

    assert ds.sizes["sid"] <= len(keep_sids)
