"""
CLI for canvodpy configuration management.

Provides commands for:
- Initializing configuration files from templates
- Validating configuration
- Viewing current configuration
- Editing configuration files
"""

import shutil
import subprocess
from pathlib import Path
from typing import Annotated

import typer
from rich.console import Console
from rich.table import Table

from .models import ProcessingConfig, SidsConfig, SitesConfig


def find_monorepo_root() -> Path:
    """Find the monorepo root by looking for a .git directory.

    Returns
    -------
    Path
        Monorepo root directory.

    Raises
    ------
    RuntimeError
        If the monorepo root cannot be found.
    """
    current = Path.cwd().resolve()

    # Walk up directory tree looking for .git
    for parent in [current] + list(current.parents):
        if (parent / ".git").exists():
            return parent

    # Fallback: if this file is in
    # packages/canvod-utils/src/canvod/utils/config/cli.py then monorepo root is
    # 7 levels up.
    try:
        cli_file = Path(__file__).resolve()
        # cli.py -> config -> utils -> canvod -> src -> canvod-utils ->
        # packages -> root.
        monorepo_root = cli_file.parent.parent.parent.parent.parent.parent.parent
        if (monorepo_root / ".git").exists():
            return monorepo_root
    except Exception:
        pass

    raise RuntimeError("Cannot find monorepo root (no .git directory found)")


# Main app
main_app = typer.Typer(
    name="canvodpy",
    help="canvodpy CLI tools",
    no_args_is_help=True,
)

# Config subcommand
config_app = typer.Typer(
    name="config",
    help="Configuration management",
    no_args_is_help=True,
)

console = Console()

# Always use monorepo root config directory
try:
    MONOREPO_ROOT = find_monorepo_root()
    DEFAULT_CONFIG_DIR = MONOREPO_ROOT / "config"
except RuntimeError:
    # Fallback if we can't find monorepo root
    DEFAULT_CONFIG_DIR = Path.cwd() / "config"

CONFIG_DIR_OPTION = typer.Option(
    "--config-dir",
    "-c",
    help="Configuration directory",
)


@config_app.command()
def init(
    config_dir: Annotated[Path, CONFIG_DIR_OPTION] = DEFAULT_CONFIG_DIR,
    force: bool = typer.Option(
        False,
        "--force",
        "-f",
        help="Overwrite existing files",
    ),
) -> None:
    """Initialize configuration files from templates.

    Creates:
      - config/processing.yaml
      - config/sites.yaml
      - config/sids.yaml

    Parameters
    ----------
    config_dir : Path
        Directory where configuration files are created.
    force : bool
        Overwrite existing files.

    Returns
    -------
    None
    """
    console.print("\n[bold]Initializing canvodpy configuration...[/bold]\n")

    # Create config directory
    config_dir.mkdir(parents=True, exist_ok=True)

    # Get template directory (from monorepo root)
    try:
        monorepo_root = find_monorepo_root()
        template_dir = monorepo_root / "config"
    except RuntimeError:
        # Fallback to path calculation if monorepo root not found
        template_dir = (
            Path(__file__).parent.parent.parent.parent.parent.parent.parent / "config"
        )

    if not template_dir.exists():
        console.print(
            f"[red]❌ Template directory not found: {template_dir}[/red]",
        )
        console.print("\nMake sure you're running from the repository root.")
        raise typer.Exit(1)

    files_created = []
    files_skipped = []

    # Copy templates
    templates = [
        ("processing.yaml.example", config_dir / "processing.yaml"),
        ("sites.yaml.example", config_dir / "sites.yaml"),
        ("sids.yaml.example", config_dir / "sids.yaml"),
    ]

    for template_name, dest_path in templates:
        template_path = template_dir / template_name

        if dest_path.exists() and not force:
            files_skipped.append(dest_path)
            continue

        if template_path.exists():
            shutil.copy(template_path, dest_path)
            files_created.append(dest_path)
        else:
            console.print(f"[yellow]⚠️  Template not found: {template_path}[/yellow]")

    # Show results
    if files_created:
        console.print("[green]✓ Created:[/green]")
        for f in files_created:
            console.print(f"  {f}")

    if files_skipped:
        console.print("\n[yellow]⊘ Skipped (already exist):[/yellow]")
        for f in files_skipped:
            console.print(f"  {f}")
        console.print("\n  Use --force to overwrite")

    # Next steps
    console.print("\n[bold]Next steps:[/bold]")
    console.print("  1. Edit config/processing.yaml:")
    console.print("     - Set nasa_earthdata_acc_mail (optional, for NASA CDDIS)")
    console.print("  2. Edit config/sites.yaml with your research sites")
    console.print("     - Set gnss_site_data_root for each site")
    console.print("  3. Run: just config-validate\n")


@config_app.command()
def validate(
    config_dir: Annotated[Path, CONFIG_DIR_OPTION] = DEFAULT_CONFIG_DIR,
) -> None:
    """Validate configuration files.

    Parameters
    ----------
    config_dir : Path
        Directory containing config files.

    Returns
    -------
    None
    """
    from .loader import load_config

    console.print("\n[bold]Validating configuration...[/bold]\n")

    try:
        config = load_config(config_dir)
        console.print("[green]✓ Configuration is valid![/green]\n")

        # Show summary
        console.print(f"  Sites: {len(config.sites.sites)}")
        for name in config.sites.sites.keys():
            console.print(f"    - {name}")

        console.print(f"\n  SID mode: {config.sids.mode}")
        console.print(f"  Agency: {config.processing.aux_data.agency}")

        # Show site data roots
        for name, site in config.sites.sites.items():
            console.print(f"  {name} data root: {site.gnss_site_data_root}")

        # Show credentials from config
        email = config.processing.credentials.nasa_earthdata_acc_mail
        if email:
            console.print(f"  NASA Earthdata email: {email}")
            console.print("  [green]✓ NASA CDDIS enabled[/green]")
        else:
            console.print("  [yellow]⊘ NASA CDDIS disabled (ESA only)[/yellow]")

        console.print()

    except Exception as e:
        console.print("[red]❌ Validation failed:[/red]\n")
        console.print(str(e))
        console.print()
        raise typer.Exit(1) from e


@config_app.command()
def show(
    config_dir: Annotated[Path, CONFIG_DIR_OPTION] = DEFAULT_CONFIG_DIR,
    section: str = typer.Option(
        None,
        "--section",
        "-s",
        help="Show specific section (processing, sites, sids)",
    ),
) -> None:
    """Display current configuration.

    Parameters
    ----------
    config_dir : Path
        Directory containing config files.
    section : str
        Optional section name (processing, sites, sids).

    Returns
    -------
    None
    """
    from .loader import load_config

    try:
        config = load_config(config_dir)
    except Exception as e:
        console.print(f"\n[red]❌ Error loading config:[/red] {e}\n")
        raise typer.Exit(1) from e

    console.print("\n[bold]Current Configuration[/bold]\n")

    if section == "processing" or section is None:
        _show_processing(config.processing)

    if section == "sites" or section is None:
        _show_sites(config.sites)

    if section == "sids" or section is None:
        _show_sids(config.sids)

    console.print()


@config_app.command()
def edit(
    file: str = typer.Argument(
        ...,
        help="Config file to edit (processing, sites, sids)",
    ),
    config_dir: Annotated[Path, CONFIG_DIR_OPTION] = DEFAULT_CONFIG_DIR,
) -> None:
    """Open a configuration file in the editor.

    Parameters
    ----------
    file : str
        Config file to edit (processing, sites, sids).
    config_dir : Path
        Directory containing config files.

    Returns
    -------
    None
    """
    import os

    file_map = {
        "processing": config_dir / "processing.yaml",
        "sites": config_dir / "sites.yaml",
        "sids": config_dir / "sids.yaml",
    }

    if file not in file_map:
        console.print(f"[red]Unknown config file:[/red] {file}")
        console.print(f"Choose from: {', '.join(file_map.keys())}")
        raise typer.Exit(1)

    file_path = file_map[file]

    if not file_path.exists():
        console.print(f"[red]File not found:[/red] {file_path}")
        console.print("\nRun: just config-init")
        raise typer.Exit(1)

    # Open in editor
    editor = os.getenv("EDITOR", "nano")
    subprocess.run([editor, str(file_path)])


def _show_processing(config: ProcessingConfig) -> None:
    """Display processing config.

    Parameters
    ----------
    config : ProcessingConfig
        Processing configuration object.

    Returns
    -------
    None
    """
    console.print("[bold]Processing Configuration:[/bold]")
    table = Table(show_header=False, padding=(0, 2))

    email = config.credentials.nasa_earthdata_acc_mail
    table.add_row(
        "NASA Earthdata Email",
        email or "[yellow]Not set (ESA only)[/yellow]",
    )
    if email:
        table.add_row("FTP Priority", "NASA CDDIS -> ESA (fallback)")
    else:
        table.add_row("FTP Priority", "ESA only")

    table.add_row("Agency", config.aux_data.agency)
    table.add_row("Product Type", config.aux_data.product_type)
    table.add_row("Max Threads", str(config.processing.n_max_threads))
    table.add_row(
        "Time Aggregation",
        f"{config.processing.time_aggregation_seconds}s",
    )
    glonass_mode = (
        "Aggregated" if config.processing.aggregate_glonass_fdma else "Individual"
    )
    table.add_row("GLONASS FDMA", glonass_mode)
    table.add_row("Keep RINEX Vars", ", ".join(config.processing.keep_rnx_vars))
    table.add_row("Batch Hours", str(config.processing.batch_hours))
    mem_str = (
        f"{config.processing.max_memory_gb} GB"
        if config.processing.max_memory_gb
        else "[dim]no limit[/dim]"
    )
    table.add_row("Max Memory", mem_str)
    affinity_str = (
        str(config.processing.cpu_affinity)
        if config.processing.cpu_affinity
        else "[dim]no restriction[/dim]"
    )
    table.add_row("CPU Affinity", affinity_str)
    table.add_row("Nice Priority", str(config.processing.nice_priority))
    table.add_row(
        "Dask Dashboard",
        "[dim]http://localhost:8787 (available when pipeline runs)[/dim]",
    )
    console.print(table)
    console.print()

    # Storage
    console.print("[bold]Storage:[/bold]")
    st = config.storage
    console.print(f"  Stores root:       {st.stores_root_dir}")
    console.print(f"  RINEX store name:  {st.rinex_store_name}")
    console.print(f"  VOD store name:    {st.vod_store_name}")
    aux_dir = str(st.aux_data_dir) if st.aux_data_dir else "[dim]system temp[/dim]"
    console.print(f"  Aux data dir:      {aux_dir}")
    console.print(
        f"  RINEX strategy:    {st.rinex_store_strategy} (expire: {st.rinex_store_expire_days}d)"
    )
    console.print(f"  VOD strategy:      {st.vod_store_strategy}")
    console.print()

    # Icechunk
    ic = config.icechunk
    console.print("[bold]Icechunk:[/bold]")
    console.print(
        f"  Compression:       {ic.compression_algorithm} (level {ic.compression_level})"
    )
    console.print(f"  Inline threshold:  {ic.inline_threshold} bytes")
    console.print(f"  Get concurrency:   {ic.get_concurrency}")
    for store_name, strategy in ic.chunk_strategies.items():
        console.print(
            f"  Chunks ({store_name}): epoch={strategy.epoch}, sid={strategy.sid}"
        )
    console.print()

    # Logging
    lg = config.logging
    console.print("[bold]Logging:[/bold]")
    log_dir = str(lg.log_dir) if lg.log_dir else "[dim]<monorepo>/.logs[/dim]"
    console.print(f"  Log directory:     {log_dir}")
    console.print(f"  Log file name:     {lg.log_file_name}")
    console.print(f"  Log path depth:    {lg.log_path_depth}")
    console.print()


def _show_sites(config: SitesConfig) -> None:
    """Display sites config.

    Parameters
    ----------
    config : SitesConfig
        Sites configuration object.

    Returns
    -------
    None
    """
    from .loader import load_config as _load_config

    try:
        full_config = _load_config()
        storage = full_config.processing.storage
    except Exception:
        storage = None

    console.print("[bold]Research Sites:[/bold]")

    for site_name, site in config.sites.items():
        base_path = site.get_base_path()

        console.print(f"\n  [bold cyan]{site_name}[/bold cyan]")
        console.print(f"    Data root: {site.gnss_site_data_root}")

        # Store paths
        if storage:
            console.print(f"    RINEX store: {storage.get_rinex_store_path(site_name)}")
            console.print(f"    VOD store:   {storage.get_vod_store_path(site_name)}")

        # Receivers table
        canopy_names = site.get_canopy_receiver_names()
        ref_names = [n for n, c in site.receivers.items() if c.type == "reference"]

        console.print(
            f"\n    [bold]Receivers[/bold] "
            f"({len(canopy_names)} canopy, {len(ref_names)} reference):"
        )

        for recv_name, recv in site.receivers.items():
            abs_dir = str(base_path / recv.directory)
            type_color = "magenta" if recv.type == "reference" else "blue"
            console.print(
                f"      [green]{recv_name}[/green] "
                f"[{type_color}]({recv.type})[/{type_color}]"
            )
            console.print(f"        dir: {abs_dir}")
            if recv.scs_from is not None:
                if recv.scs_from == "all":
                    console.print(f"        scs_from: all -> {canopy_names}")
                else:
                    console.print(f"        scs_from: {recv.scs_from}")

        # Reference-canopy pairs (expanded from scs_from)
        pairs = site.get_reference_canopy_pairs()
        if pairs:
            console.print(
                f"\n    [bold]Reference x Canopy store groups[/bold] ({len(pairs)}):"
            )
            pair_table = Table(
                show_header=True, padding=(0, 1), box=None, pad_edge=False
            )
            pair_table.add_column("Store Group", style="green")
            pair_table.add_column("Reference")
            pair_table.add_column("Position From")

            for ref_name, canopy_name in pairs:
                group_name = f"{ref_name}_{canopy_name}"
                pair_table.add_row(group_name, ref_name, canopy_name)

            console.print(pair_table)

        # VOD analyses
        if site.vod_analyses:
            console.print(
                f"\n    [bold]VOD Analyses[/bold] ({len(site.vod_analyses)}):"
            )
            vod_table = Table(
                show_header=True, padding=(0, 1), box=None, pad_edge=False
            )
            vod_table.add_column("Name", style="green")
            vod_table.add_column("Canopy")
            vod_table.add_column("Reference")

            for analysis_name, analysis in site.vod_analyses.items():
                vod_table.add_row(
                    analysis_name,
                    analysis.canopy_receiver,
                    analysis.reference_receiver,
                )

            console.print(vod_table)


def _show_sids(config: SidsConfig) -> None:
    """Display SIDs config.

    Parameters
    ----------
    config : SidsConfig
        SIDs configuration object.

    Returns
    -------
    None
    """
    console.print("[bold]Signal IDs:[/bold]")
    table = Table(show_header=False)
    table.add_row("Mode", config.mode)
    if config.mode == "preset":
        table.add_row("Preset", config.preset or "")
    elif config.mode == "custom":
        table.add_row("Custom SIDs", f"{len(config.custom_sids)} defined")
    console.print(table)
    console.print()


# Register config subcommand
main_app.add_typer(config_app, name="config")


def main() -> None:
    """Run the CLI entry point."""
    main_app()


if __name__ == "__main__":
    main()
