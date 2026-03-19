"""RINEX v3 observation file trimmer and concatenator.

Wraps ``gfzrnx`` (GFZ RINEX toolbox) to produce controlled input files
for cross-tool comparison. By stripping the RINEX down to specific
satellite systems and observation codes, we eliminate ambiguity about
which signal each tool selects — essential for Tier 3 audits.

``gfzrnx`` is the IGS community standard for RINEX manipulation.
Download from: https://gnss.gfz-potsdam.de/gfzrnx

Usage::

    from canvod.audit.rinex_trimmer import RinexTrimmer

    trimmer = RinexTrimmer(
        keep_systems=["G", "E"],
        keep_obs_codes={
            "G": ["C1C", "L1C", "S1C", "C2W", "L2W", "S2W"],
            "E": ["C1C", "L1C", "S1C", "C5Q", "L5Q", "S5Q"],
        },
    )

    # Show what will be kept vs dropped
    trimmer.preview(input_files)

    # Write trimmed + concatenated file
    trimmer.write(input_files, output_path)
"""

from __future__ import annotations

import re
import shutil
import subprocess
from pathlib import Path


def _find_gfzrnx():
    """Find gfzrnx binary. Raises RuntimeError if not found."""
    path = shutil.which("gfzrnx")
    if path is None:
        raise RuntimeError(
            "gfzrnx not found on PATH.\n"
            "Download from: https://gnss.gfz-potsdam.de/gfzrnx\n"
            "It is free for academic/non-commercial use."
        )
    return path


def _parse_obs_types_from_header(fpath):
    """Parse SYS / # / OBS TYPES from a RINEX v3 header.

    Returns dict mapping system letter → list of 3-char obs codes.
    """
    obs_types = {}
    current_system = None

    with open(fpath) as f:
        for line in f:
            if "END OF HEADER" in line[60:]:
                break
            if "SYS / # / OBS TYPES" not in line[60:]:
                continue

            if line[0] != " ":
                current_system = line[0]
                rest = line[6:60]
                # Strip Septentrio's proprietary "X1" field
                rest = re.sub(r"^\s*X\d\s*", "", rest)
                codes = [c for c in rest.split() if len(c) == 3]
                obs_types[current_system] = codes
            else:
                if current_system is not None:
                    codes = [c for c in line[6:60].split() if len(c) == 3]
                    obs_types[current_system].extend(codes)

    return obs_types


class RinexTrimmer:
    """Trim and concatenate RINEX v3 observation files using gfzrnx.

    Parameters
    ----------
    keep_systems : list of str
        GNSS system letters to retain, e.g. ["G", "E"].
    keep_obs_codes : dict
        Maps system letter → list of 3-char RINEX obs codes to retain.
        Example: {"G": ["C1C", "L1C", "S1C"], "E": ["C1C", "L1C", "S1C"]}
    """

    def __init__(self, keep_systems, keep_obs_codes):
        self.keep_systems = sorted(keep_systems)
        self.keep_obs_codes = {s: list(codes) for s, codes in keep_obs_codes.items()}

        for s in self.keep_systems:
            if s not in self.keep_obs_codes:
                raise ValueError(
                    f"System '{s}' is in keep_systems but has no "
                    f"obs codes defined in keep_obs_codes."
                )

    def preview(self, input_files):
        """Print a summary of what will be kept vs dropped.

        Parameters
        ----------
        input_files : list of str or Path
            RINEX files to inspect (only the first file's header is used).
        """
        fpath = Path(input_files[0])
        original = _parse_obs_types_from_header(fpath)

        print(f"Input: {len(input_files)} files")
        print(f"Reference header: {fpath.name}")
        print()

        for system in sorted(set(list(original.keys()) + self.keep_systems)):
            orig_codes = original.get(system, [])
            keep_codes = self.keep_obs_codes.get(system, [])

            if system not in self.keep_systems:
                print(f"  {system}: DROP entire system ({len(orig_codes)} obs codes)")
                continue

            kept = [c for c in orig_codes if c in keep_codes]
            dropped = [c for c in orig_codes if c not in keep_codes]
            missing = [c for c in keep_codes if c not in orig_codes]

            print(f"  {system}: {len(orig_codes)} → {len(kept)} obs codes")
            if kept:
                print(f"    KEEP:    {' '.join(kept)}")
            if dropped:
                print(f"    DROP:    {' '.join(dropped)}")
            if missing:
                print(f"    MISSING: {' '.join(missing)} (requested but not in file)")

    def write(self, input_files, output_path):
        """Write a trimmed, concatenated RINEX file.

        Uses gfzrnx to filter satellite systems and observation codes,
        and to splice (concatenate) multiple input files.

        Parameters
        ----------
        input_files : list of str or Path
            Input RINEX files, in chronological order.
        output_path : str or Path
            Where to write the output file.

        Returns
        -------
        Path
            The output file path.
        """
        gfzrnx = _find_gfzrnx()
        input_files = sorted(Path(f) for f in input_files)
        output_path = Path(output_path)
        output_path.parent.mkdir(parents=True, exist_ok=True)

        # Build gfzrnx command
        cmd = [gfzrnx]

        # Input files
        for f in input_files:
            cmd.extend(["-finp", str(f)])

        # Output file
        cmd.extend(["-fout", str(output_path)])

        # Filter satellite systems: -satsys GE
        cmd.extend(["-satsys", "".join(self.keep_systems)])

        # Filter obs types: -obs_types G:C1C,L1C,S1C+E:C1C,L1C,S1C
        obs_parts = []
        for system in self.keep_systems:
            codes = ",".join(self.keep_obs_codes[system])
            obs_parts.append(f"{system}:{codes}")
        cmd.extend(["-obs_types", "+".join(obs_parts)])

        # Splice mode (concatenate, no RAM buffering)
        cmd.append("-splice_direct")

        print(f"Running: {' '.join(cmd[:6])} ... ({len(input_files)} files)")
        print(f"  Systems: {', '.join(self.keep_systems)}")
        for s in self.keep_systems:
            print(f"  {s}: {' '.join(self.keep_obs_codes[s])}")

        result = subprocess.run(
            cmd,
            capture_output=True,
            text=True,
        )

        if result.returncode != 0:
            raise RuntimeError(
                f"gfzrnx failed (exit {result.returncode}):\n"
                f"stdout: {result.stdout}\n"
                f"stderr: {result.stderr}"
            )

        if result.stderr:
            # gfzrnx prints info/warnings to stderr
            for line in result.stderr.strip().splitlines():
                print(f"  gfzrnx: {line}")

        if not output_path.exists():
            raise RuntimeError(
                f"gfzrnx completed but output file not found: {output_path}\n"
                f"stdout: {result.stdout}\n"
                f"stderr: {result.stderr}"
            )

        size_mb = output_path.stat().st_size / 1024 / 1024
        print(f"Written: {output_path} ({size_mb:.1f} MB)")

        return output_path

    def gfzrnx_command(self, input_files, output_path):
        """Return the gfzrnx command as a list (for logging / reproducibility).

        Same arguments as write(), but does not execute anything.
        """
        input_files = sorted(Path(f) for f in input_files)
        output_path = Path(output_path)

        cmd = ["gfzrnx"]
        for f in input_files:
            cmd.extend(["-finp", str(f)])
        cmd.extend(["-fout", str(output_path)])
        cmd.extend(["-satsys", "".join(self.keep_systems)])

        obs_parts = []
        for system in self.keep_systems:
            codes = ",".join(self.keep_obs_codes[system])
            obs_parts.append(f"{system}:{codes}")
        cmd.extend(["-obs_types", "+".join(obs_parts)])
        cmd.append("-splice_direct")

        return cmd

    def describe(self, input_files, output_path):
        """Return a human-readable description of the trimming operation.

        Suitable for inclusion in a methods section or reproducibility log.

        Parameters
        ----------
        input_files : list of str or Path
            Input RINEX files.
        output_path : str or Path
            Output file path.

        Returns
        -------
        str
            Multi-line description.
        """
        input_files = sorted(Path(f) for f in input_files)
        output_path = Path(output_path)
        cmd = self.gfzrnx_command(input_files, output_path)

        lines = [
            "RINEX Observation File Preparation",
            "=" * 40,
            "",
            "Tool: gfzrnx (GFZ Helmholtz-Centre for Geosciences)",
            f"Input: {len(input_files)} RINEX v3.04 observation files",
            f"  First: {input_files[0].name}",
            f"  Last:  {input_files[-1].name}",
            f"Output: {output_path.name}",
            "",
            "Satellite systems retained:",
        ]
        for s in self.keep_systems:
            codes = self.keep_obs_codes[s]
            lines.append(f"  {s}: {', '.join(codes)}")

        systems_dropped = []
        try:
            original = _parse_obs_types_from_header(input_files[0])
            systems_dropped = [s for s in original if s not in self.keep_systems]
        except Exception:
            pass

        if systems_dropped:
            lines.append(f"Satellite systems removed: {', '.join(systems_dropped)}")

        lines.extend(
            [
                "",
                "Rationale:",
                "  The RINEX file is trimmed to one observation code per frequency",
                "  band per satellite system. This ensures unambiguous signal",
                "  selection when the same file is processed by tools with",
                "  different multi-code handling strategies. canvodpy indexes",
                "  observations by Signal ID (PRN|band|code), while gnssvod",
                "  merges codes within a band via fillna (lexicographic priority,",
                "  side-effect of numpy.intersect1d). By providing a single code",
                "  per band, both tools produce directly comparable PRN-level",
                "  output without post-hoc signal selection logic.",
                "",
                "Reproducibility:",
                f"  $ {' '.join(str(c) for c in cmd)}",
            ]
        )

        return "\n".join(lines)


# ---------------------------------------------------------------------------
# Ready-made configs for common comparison scenarios
# ---------------------------------------------------------------------------


def gps_galileo_l1_l2():
    """GPS + Galileo, one code per band — ideal for gnssvod comparison."""
    return RinexTrimmer(
        keep_systems=["G", "E"],
        keep_obs_codes={
            "G": ["C1C", "L1C", "S1C", "C2W", "L2W", "S2W"],
            "E": ["C1C", "L1C", "S1C", "C5Q", "L5Q", "S5Q"],
        },
    )


def gps_l1_only():
    """GPS only, L1 C/A only — simplest possible comparison."""
    return RinexTrimmer(
        keep_systems=["G"],
        keep_obs_codes={
            "G": ["C1C", "L1C", "S1C"],
        },
    )


# ---------------------------------------------------------------------------
# Auto-detection: build trimmer from RINEX header
# ---------------------------------------------------------------------------

# RINEX observation attribute prefixes: C=code, L=phase, S=SNR, D=Doppler
_OBS_ATTRS = ("C", "L", "S", "D")


def _detect_bands(obs_codes: list[str]) -> dict[str, list[str]]:
    """Group observation codes by frequency band.

    Returns dict mapping band number → list of tracking codes for that
    band. For example, if obs_codes = ["C1C", "C1W", "S1C", "S1W", "C2W"],
    returns {"1": ["C", "W"], "2": ["W"]}.

    Only considers unique tracking codes (the 3rd character) across all
    observation attributes (C, L, S, D).
    """
    bands: dict[str, set[str]] = {}
    for code in obs_codes:
        if len(code) != 3:
            continue
        band_num = code[1]
        tracking = code[2]
        bands.setdefault(band_num, set()).add(tracking)
    return {b: sorted(codes) for b, codes in sorted(bands.items())}


def auto_trimmer_from_header(
    rinex_file,
    *,
    keep_systems: list[str] | None = None,
    prefer_codes: list[str] | None = None,
) -> RinexTrimmer:
    """Build a RinexTrimmer by auto-detecting obs codes from a RINEX header.

    For each system and band, selects exactly one tracking code. Selection
    priority:

    1. ``prefer_codes`` list (if given): first match wins
    2. gnssvod-compatible (lexicographic first): ensures the trimmed file
       produces the same result as gnssvod's fillna merge

    This guarantees one code per band, making canvodpy SIDs map 1:1 to
    gnssvod PRNs, without hardcoding specific code choices.

    Parameters
    ----------
    rinex_file : str or Path
        Path to a RINEX v3 file to inspect.
    keep_systems : list of str, optional
        Systems to retain. Default: all systems in the file.
    prefer_codes : list of str, optional
        Preferred tracking codes in priority order, e.g. ``["C", "W", "Q"]``.
        Default: lexicographic (matches gnssvod's numpy.intersect1d order).

    Returns
    -------
    RinexTrimmer
        Configured to keep one code per band per system.

    Examples
    --------
    >>> # Auto-detect, matching gnssvod's code selection
    >>> trimmer = auto_trimmer_from_header("input.rnx")
    >>> trimmer.preview(["input.rnx"])

    >>> # Force specific systems
    >>> trimmer = auto_trimmer_from_header("input.rnx", keep_systems=["G", "E"])

    >>> # Prefer W codes where available (e.g. P(Y) tracking)
    >>> trimmer = auto_trimmer_from_header("input.rnx", prefer_codes=["W", "C"])
    """
    from pathlib import Path

    rinex_file = Path(rinex_file)
    header_obs = _parse_obs_types_from_header(rinex_file)

    if keep_systems is None:
        keep_systems = sorted(header_obs.keys())
    else:
        keep_systems = sorted(keep_systems)

    keep_obs_codes: dict[str, list[str]] = {}
    for system in keep_systems:
        sys_codes = header_obs.get(system, [])
        if not sys_codes:
            continue

        bands = _detect_bands(sys_codes)
        selected: list[str] = []

        for band_num, tracking_codes in bands.items():
            # Pick one tracking code for this band
            chosen = _pick_code(tracking_codes, prefer_codes)

            # Keep all observation attributes (C, L, S, D) for this code
            for attr in _OBS_ATTRS:
                full_code = f"{attr}{band_num}{chosen}"
                if full_code in sys_codes:
                    selected.append(full_code)

        if selected:
            keep_obs_codes[system] = selected

    # Only keep systems that have codes
    keep_systems = [s for s in keep_systems if s in keep_obs_codes]

    if not keep_systems:
        raise ValueError(
            f"No observation codes found for systems {keep_systems} "
            f"in {rinex_file.name}. Available: {header_obs}"
        )

    return RinexTrimmer(keep_systems=keep_systems, keep_obs_codes=keep_obs_codes)


def _pick_code(
    available: list[str],
    prefer: list[str] | None,
) -> str:
    """Pick one tracking code from available options.

    If prefer is given, uses first match. Otherwise lexicographic first
    (matches gnssvod's numpy.intersect1d sort order).
    """
    if prefer:
        for code in prefer:
            if code in available:
                return code
    # Lexicographic = gnssvod default
    return available[0]


def random_trimmer_from_header(
    rinex_file,
    *,
    keep_systems: list[str] | None = None,
    seed: int = 42,
) -> RinexTrimmer:
    """Build a RinexTrimmer with randomly selected codes per band.

    For robustness testing: verifies that the comparison works regardless
    of which tracking code is selected. If the audit passes with random
    codes, it proves the comparison logic is code-agnostic.

    Parameters
    ----------
    rinex_file : str or Path
        Path to a RINEX v3 file to inspect.
    keep_systems : list of str, optional
        Systems to retain.
    seed : int
        Random seed for reproducibility.

    Returns
    -------
    RinexTrimmer
    """
    import random
    from pathlib import Path

    rinex_file = Path(rinex_file)
    header_obs = _parse_obs_types_from_header(rinex_file)

    if keep_systems is None:
        keep_systems = sorted(header_obs.keys())
    else:
        keep_systems = sorted(keep_systems)

    rng = random.Random(seed)
    keep_obs_codes: dict[str, list[str]] = {}

    for system in keep_systems:
        sys_codes = header_obs.get(system, [])
        if not sys_codes:
            continue

        bands = _detect_bands(sys_codes)
        selected: list[str] = []

        for band_num, tracking_codes in bands.items():
            chosen = rng.choice(tracking_codes)
            for attr in _OBS_ATTRS:
                full_code = f"{attr}{band_num}{chosen}"
                if full_code in sys_codes:
                    selected.append(full_code)

        if selected:
            keep_obs_codes[system] = selected

    keep_systems = [s for s in keep_systems if s in keep_obs_codes]
    return RinexTrimmer(keep_systems=keep_systems, keep_obs_codes=keep_obs_codes)
