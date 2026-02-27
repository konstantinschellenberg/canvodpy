"""Clock correction data handling.

This module provides tools for reading, parsing, and validating satellite
clock correction data from RINEX CLK format files.
"""

from canvod.auxiliary.clock.parser import (
    parse_clk_data,
    parse_clk_file,
    parse_clk_header,
)
from canvod.auxiliary.clock.reader import ClkFile
from canvod.auxiliary.clock.validator import (
    check_clk_data_quality,
    validate_clk_dataset,
)

__all__ = [
    "ClkFile",
    "check_clk_data_quality",
    "parse_clk_data",
    "parse_clk_file",
    "parse_clk_header",
    "validate_clk_dataset",
]
