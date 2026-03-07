"""Create a 1-day RINEX store and exercise metadata lifecycle.

This is an integration test that requires:
- RINEX files at /Volumes/ExtremePro/Sample_data/02_canopy/25001/
- canvod-readers installed

Run with: uv run pytest packages/canvod-store-metadata/tests/test_1d_store.py -v -s
"""

import shutil
from pathlib import Path

import pytest

SAMPLE_DIR = Path("/Volumes/ExtremePro/Sample_data/02_canopy/25001")
SKIP_REASON = "Sample RINEX data not available"


@pytest.mark.integration
class TestOneDayStore:
    """Build a real 1-day store, write metadata, query it."""

    @pytest.fixture
    def store_path(self, tmp_path):
        """Create a temp store path, clean up after."""
        sp = tmp_path / "test_1d_rinex"
        yield sp
        if sp.exists():
            shutil.rmtree(sp)

    @pytest.mark.skipif(not SAMPLE_DIR.exists(), reason=SKIP_REASON)
    def test_build_1d_store_with_metadata(self, store_path):
        import icechunk
        import xarray as xr
        from canvod.readers.rinex.v3_04 import Rnxv3Obs

        from canvod.store_metadata import (
            format_metadata,
            read_metadata,
            validate_all,
            write_metadata,
        )
        from canvod.store_metadata.collectors import (
            collect_environment,
            collect_processing_provenance,
        )
        from canvod.store_metadata.schema import (
            Creator,
            SiteInfo,
            SpatialExtent,
            StoreIdentity,
            StoreMetadata,
            Summaries,
            TemporalExtent,
        )

        # ── 1. Read 4 × 15-min RINEX files (first hour) ────────────
        obs_files = sorted(SAMPLE_DIR.glob("*.25o"))[:4]
        assert len(obs_files) >= 4, f"Need ≥4 obs files, got {len(obs_files)}"

        datasets = []
        for f in obs_files:
            reader = Rnxv3Obs(f)
            ds = reader.to_dataset()
            datasets.append(ds)

        combined = xr.concat(datasets, dim="epoch")
        n_epochs = len(combined.epoch)
        n_sids = len(combined.sid)
        variables = list(combined.data_vars)

        print(
            f"\nRead {len(obs_files)} files: "
            f"{n_epochs} epochs x {n_sids} sids, "
            f"vars={variables}"
        )

        # ── 2. Create Icechunk store and write data ─────────────────
        storage = icechunk.local_filesystem_storage(str(store_path))
        repo = icechunk.Repository.create(storage=storage)
        session = repo.writable_session("main")
        combined.to_zarr(
            session.store,
            group="canopy_01",
            mode="w",
        )
        snapshot = session.commit("Write 1-day canopy data")
        print(f"Store created at {store_path} (snapshot: {snapshot[:8]})")

        # ── 3. Collect and write metadata ───────────────────────────
        time_start = str(combined.epoch.values[0])
        time_end = str(combined.epoch.values[-1])

        meta = StoreMetadata(
            identity=StoreIdentity(
                id="Rosalia/rinex_store",
                title="Rosalia Rinex Store (1-day test)",
                description="1-hour subset of GNSS observations "
                "from Rosalia canopy receiver",
                store_type="rinex_store",
                source_format="rinex3",
                keywords=["GNSS", "VOD", "Rosalia", "RINEX", "test"],
                naming_authority="at.ac.tuwien",
            ),
            creator=Creator(
                name="Nicolas François Bader",
                email="nicolas.bader@tuwien.ac.at",
                institution="TU Wien",
                institution_ror="https://ror.org/04d836q62",
                department="Geodesy and Geoinformation",
                research_group="CLIMERS",
                website="https://www.tuwien.at/en/mg/geo/climers",
            ),
            temporal=TemporalExtent(
                created="2026-03-07T00:00:00Z",
                updated="2026-03-07T00:00:00Z",
                collected_start=time_start,
                collected_end=time_end,
            ),
            spatial=SpatialExtent(
                site=SiteInfo(
                    name="Rosalia",
                    description="Mixed forest research site",
                    country="AT",
                ),
                geospatial_lat=47.7,
                geospatial_lon=16.3,
                geospatial_alt_m=680.0,
                bbox=[16.3, 47.7, 16.3, 47.7],
                extent_temporal_interval=[[time_start, time_end]],
            ),
            processing=collect_processing_provenance("rinex_store", "rinex3"),
            environment=collect_environment(store_path),
            summaries=Summaries(
                total_epochs=n_epochs,
                total_sids=n_sids,
                variables=variables,
                file_count=len(obs_files),
                history=["2026-03-07: 1-day test store created"],
            ),
        )

        write_metadata(store_path, meta)
        print("Metadata written to store")

        # ── 4. Read back and verify ─────────────────────────────────
        restored = read_metadata(store_path)
        assert restored.identity.id == "Rosalia/rinex_store"
        assert restored.summaries.total_epochs == n_epochs
        assert restored.environment.uv_lock_hash is not None
        assert restored.environment.pyproject_toml_text is not None
        assert restored.environment.uv_lock_text is not None
        print("Round-trip verified")

        # ── 5. Show the full report ─────────────────────────────────
        report = format_metadata(restored)
        print("\n" + report)

        # ── 6. Show individual sections ─────────────────────────────
        for section in ["identity", "env", "summaries", "validation"]:
            print(f"\n--- {section} ---")
            print(format_metadata(restored, section=section))

        # ── 7. Reproduce instructions ──────────────────────────────
        print("\n--- reproduce ---")
        print(format_metadata(restored, section="reproduce"))

        # ── 8. Validate ────────────────────────────────────────────
        results = validate_all(restored)
        print("\nValidation:")
        for std, issues in results.items():
            status = "PASS" if not issues else f"{len(issues)} issues"
            print(f"  {std}: {status}")

        # ── 9. Verify env can be extracted ──────────────────────────
        from canvod.store_metadata.show import extract_env

        env_dir = store_path.parent / "repro_env"
        extract_env(store_path, env_dir)
        assert (env_dir / "pyproject.toml").exists()
        assert (env_dir / "uv.lock").exists()
        toml_text = (env_dir / "pyproject.toml").read_text()
        assert "[tool.uv.workspace]" in toml_text
        print(f"\nEnv extracted to {env_dir}")
        print("All checks passed!")
