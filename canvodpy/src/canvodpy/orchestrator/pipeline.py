"""Processing pipeline orchestration for site-level workflows."""

from collections import defaultdict
from collections.abc import Generator
from pathlib import Path

import xarray as xr
from canvod.readers import MatchedDirs, PairDataDirMatcher
from canvod.store import GnssResearchSite
from canvod.utils.tools import YYYYDOY

from canvodpy.logging import get_logger
from canvodpy.orchestrator.processor import RinexDataProcessor


class PipelineOrchestrator:
    """Orchestrate RINEX processing pipeline for all receiver pairs at a site.

    Processes each unique receiver once per day, regardless of how many
    pairs it's involved in.

    Parameters
    ----------
    site : GnssResearchSite
        Research site configuration
    n_max_workers : int
        Maximum parallel workers per day
    dry_run : bool
        If True, only simulate processing without executing

    """

    def __init__(
        self,
        site: GnssResearchSite,
        n_max_workers: int = 12,
        dry_run: bool = False,
        reader_name: str = "rinex3",
    ) -> None:
        self.site = site
        self.n_max_workers = n_max_workers
        self.dry_run = dry_run
        self.reader_name = reader_name
        self._logger = get_logger(__name__).bind(site=site.site_name)

        self.pair_matcher = PairDataDirMatcher(
            base_dir=site.site_config["gnss_site_data_root"],
            receivers=site.receivers,
            analysis_pairs=site.vod_analyses,
        )

        self._logger.info(
            "pipeline_initialized",
            site=site.site_name,
            analysis_pairs=len(site.active_vod_analyses),
            n_max_workers=n_max_workers,
            dry_run=dry_run,
        )

    def _group_by_date_and_receiver(
        self,
    ) -> dict[str, dict[str, tuple[Path, str, Path | None]]]:
        """Group receivers by date, expanding references per canopy via scs_from.

        Canopy receivers are deduplicated (processed once with own position).
        Reference receivers are expanded: one entry per canopy in scs_from,
        stored as ``{ref_name}_{canopy_name}`` with position_data_dir pointing
        to the canopy's RINEX directory.

        Returns
        -------
        dict[str, dict[str, tuple[Path, str, Path | None]]]
            {date: {store_group_name: (data_dir, receiver_type, position_data_dir)}}

        """
        grouped: dict[str, dict[str, tuple[Path, str, Path | None]]] = defaultdict(dict)
        site_config = self.site._site_config

        for pair_dirs in self.pair_matcher:
            date_key = pair_dirs.yyyydoy.to_str()

            # Add canopy receiver if not already present (uses own position)
            if pair_dirs.canopy_receiver not in grouped[date_key]:
                grouped[date_key][pair_dirs.canopy_receiver] = (
                    pair_dirs.canopy_data_dir,
                    "canopy",
                    None,
                )

            # Expand reference receiver per canopy in scs_from
            ref_name = pair_dirs.reference_receiver
            ref_cfg = site_config.receivers.get(ref_name)
            if ref_cfg and ref_cfg.type == "reference":
                canopy_names = site_config.resolve_scs_from(ref_name)
                for canopy_name in canopy_names:
                    store_group = f"{ref_name}_{canopy_name}"
                    if store_group not in grouped[date_key]:
                        # Get canopy data dir for position computation
                        canopy_cfg = site_config.receivers.get(canopy_name)
                        if canopy_cfg:
                            canopy_position_dir = (
                                site_config.get_base_path()
                                / canopy_cfg.directory
                                / pair_dirs.yyyydoy.yydoy
                            )
                        else:
                            canopy_position_dir = None
                        grouped[date_key][store_group] = (
                            pair_dirs.reference_data_dir,
                            "reference",
                            canopy_position_dir,
                        )

        return grouped

    def preview_processing_plan(self) -> dict:
        """Preview what would be processed without executing.

        Returns
        -------
        dict
            Summary of dates, receivers, and files to process

        """
        grouped = self._group_by_date_and_receiver()

        plan = {
            "site": self.site.site_name,
            "dates": [],
            "total_receivers": 0,
            "total_files": 0,
        }

        for date_key, receivers in sorted(grouped.items()):
            date_info = {"date": date_key, "receivers": []}

            for receiver_name, (data_dir, receiver_type, _pos_dir) in sorted(
                receivers.items()
            ):
                files = list(data_dir.glob("*.2*o"))

                receiver_info = {
                    "name": receiver_name,
                    "type": receiver_type,
                    "files": len(files),
                    "dir": str(data_dir),
                }

                date_info["receivers"].append(receiver_info)
                plan["total_files"] += len(files)

            plan["dates"].append(date_info)
            plan["total_receivers"] += len(receivers)

        return plan

    def print_preview(self) -> None:
        """Print a formatted preview of the processing plan."""
        plan = self.preview_processing_plan()

        print(f"\n{'=' * 70}")
        print(f"PROCESSING PLAN FOR SITE: {plan['site']}")
        print(f"{'=' * 70}")
        print(f"Total unique receivers to process: {plan['total_receivers']}")
        print(f"Total RINEX files: {plan['total_files']}")
        print(f"{'=' * 70}\n")

        for date_info in plan["dates"]:
            print(f"Date: {date_info['date']}")
            for receiver_info in date_info["receivers"]:
                print(
                    f"  {receiver_info['name']} ({receiver_info['type']}): "
                    f"{receiver_info['files']} files"
                )
                print(f"    {receiver_info['dir']}")
            print()

    def process_by_date(
        self,
        keep_vars: list[str] | None = None,
        start_from: str | None = None,
        end_at: str | None = None,
    ) -> Generator[tuple[str, dict[str, xr.Dataset], dict[str, float]], None, None]:
        """Process all receivers grouped by date.

        Each unique receiver is processed once per day with its actual name
        as the Icechunk group name.

        Parameters
        ----------
        keep_vars : list[str], optional
            Variables to keep in datasets
        start_from : str, optional
            YYYYDOY string to start from
        end_at : str, optional
            YYYYDOY string to end at

        Yields
        ------
        tuple[str, dict[str, xr.Dataset]]
            Date string and dict of {receiver_name: dataset}

        """
        if self.dry_run:
            self._logger.info(
                "dry_run_mode", message="Simulating processing without execution"
            )
            self.print_preview()
            return

        grouped = self._group_by_date_and_receiver()

        for date_key, receivers in sorted(grouped.items()):
            # Filter dates before processing
            if start_from and date_key < start_from:
                self._logger.info(
                    "date_skipped_before_range",
                    date=date_key,
                    start_from=start_from,
                )
                continue

            if end_at and date_key > end_at:
                self._logger.info(
                    "date_range_complete",
                    date=date_key,
                    end_at=end_at,
                )
                break

            self._logger.info(
                "date_processing_started",
                date=date_key,
                receivers=len(receivers),
                receiver_names=sorted(receivers.keys()),
            )

            # Build receiver_configs for this date (4-tuples with position_data_dir)
            receiver_configs = [
                (receiver_name, receiver_type, data_dir, position_data_dir)
                for receiver_name, (
                    data_dir,
                    receiver_type,
                    position_data_dir,
                ) in sorted(receivers.items())
            ]

            # Convert to MatchedDirs for aux data (use any dir, aux is date-based)
            first_data_dir = receiver_configs[0][2]
            matched_dirs = MatchedDirs(
                canopy_data_dir=first_data_dir,
                reference_data_dir=first_data_dir,  # Dummy, only date matters for aux
                yyyydoy=YYYYDOY.from_str(date_key),
            )

            # Process all receivers for this date in one go
            try:
                processor = RinexDataProcessor(
                    matched_data_dirs=matched_dirs,
                    site=self.site,
                    n_max_workers=self.n_max_workers,
                    reader_name=self.reader_name,
                )
            except RuntimeError as e:
                if "Failed to download" in str(e):
                    self._logger.warning(
                        "auxiliary_download_failed",
                        date=date_key,
                        error=str(e),
                        exception=type(e).__name__,
                    )
                    continue
                else:
                    raise

            # Process with actual receiver names and directories
            datasets = {}
            timings = {}
            try:
                for receiver_name, ds, proc_time in processor.parsed_rinex_data_gen(
                    keep_vars=keep_vars, receiver_configs=receiver_configs
                ):
                    # Receiver name is already correct from the generator.
                    datasets[receiver_name] = ds
                    timings[receiver_name] = proc_time
            except (OSError, RuntimeError, ValueError) as e:
                self._logger.error(
                    "rinex_processing_failed",
                    date=date_key,
                    error=str(e),
                    exception=type(e).__name__,
                )
                continue

            yield date_key, datasets, timings


class SingleReceiverProcessor:
    """Process a single receiver for one day.

    Parameters
    ----------
    receiver_name : str
        Actual receiver name (e.g., 'canopy_01', 'reference_01')
    receiver_type : str
        Receiver type ('canopy' or 'reference')
    data_dir : Path
        Directory containing RINEX files
    yyyydoy : YYYYDOY
        Date to process
    site : GnssResearchSite
        Research site
    n_max_workers : int
        Maximum parallel workers

    """

    def __init__(
        self,
        receiver_name: str,
        receiver_type: str,
        data_dir: Path,
        yyyydoy: YYYYDOY,
        site: GnssResearchSite,
        n_max_workers: int = 12,
    ) -> None:
        self.receiver_name = receiver_name
        self.receiver_type = receiver_type
        self.data_dir = data_dir
        self.yyyydoy = yyyydoy
        self.site = site
        self.n_max_workers = n_max_workers
        self._logger = get_logger().bind(
            receiver=receiver_name,
            date=yyyydoy.to_str(),
        )

    def _get_rinex_files(self) -> list[Path]:
        """Get sorted list of RINEX files."""
        return sorted(self.data_dir.glob("*.2*o"))

    def process(self, keep_vars: list[str] | None = None) -> xr.Dataset:
        """Process all RINEX files for this receiver and write to Icechunk.

        Parameters
        ----------
        keep_vars : list[str], optional
            Variables to keep in datasets

        Returns
        -------
        xr.Dataset
            Final daily dataset for this receiver

        """
        rinex_files = self._get_rinex_files()

        if not rinex_files:
            self._logger.error(
                "no_rinex_files_found",
                data_dir=str(self.data_dir),
            )
            msg = f"No RINEX files found in {self.data_dir}"
            raise ValueError(msg)

        self._logger.info(
            "receiver_processing_started",
            rinex_files=len(rinex_files),
        )

        # Create matched dirs for aux data (using first available dir as dummy)
        matched_dirs = MatchedDirs(
            canopy_data_dir=self.data_dir,
            reference_data_dir=self.data_dir,  # Dummy, aux data is date-based
            yyyydoy=self.yyyydoy,
        )

        # Initialize processor with receiver name override
        processor = RinexDataProcessor(
            matched_data_dirs=matched_dirs,
            site=self.site,
            n_max_workers=self.n_max_workers,
            reader_name=self.reader_name,
        )

        # Process with actual receiver name (NOT type)
        # This requires modifying RinexDataProcessor to accept receiver_name parameter
        return processor._process_receiver(
            rinex_files=rinex_files,
            receiver_name=self.receiver_name,  # Use actual name as group
            receiver_type=self.receiver_type,
            keep_vars=keep_vars,
        )


if __name__ == "__main__":
    from canvod.store import GnssResearchSite
    from canvod.utils.config import load_config

    cfg = load_config()
    site = GnssResearchSite(site_name="Rosalia")

    # Test with dry run first
    orchestrator = PipelineOrchestrator(
        site=site,
        dry_run=False,
    )

    # Process all dates
    keep_vars = cfg.processing.processing.keep_rnx_vars
    for date_key, datasets in orchestrator.process_by_date(keep_vars=keep_vars):
        print(f"\nProcessed date: {date_key}")
        for receiver_name, ds in datasets.items():
            print(f"  {receiver_name}: {dict(ds.sizes)}")
