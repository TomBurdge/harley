from __future__ import annotations

from pathlib import Path
from typing import TYPE_CHECKING

import polars as pl

from harley.utils import parse_into_expr, register_plugin, parse_version

if TYPE_CHECKING:
    from polars.type_aliases import IntoExpr

if parse_version(pl.__version__) < parse_version("0.20.16"):
    from polars.utils.udfs import _get_shared_lib_location

    lib: str | Path = _get_shared_lib_location(__file__)
else:
    lib = Path(__file__).parent


def single_space(expr: IntoExpr) -> IntoExpr:
    """
    Replaces all whitespace to a single space from a string, then trims leading and trailing spaces.
    """
    expr = parse_into_expr(expr)
    return register_plugin(
        args=[expr],
        symbol="single_space",
        is_elementwise=True,
        lib=lib,
    )

def remove_all_whitespace(expr: IntoExpr) -> IntoExpr:
    """
    Removes all whitespace from a string.
    """
    expr = parse_into_expr(expr)
    return register_plugin(
        args=[expr],
        symbol="remove_all_whitespace",
        is_elementwise=True,
        lib=lib,
    )
