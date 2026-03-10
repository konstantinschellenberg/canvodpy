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
