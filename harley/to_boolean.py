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


def is_null_or_blank(expr: IntoExpr, all_white_space_as_null: bool = False) -> IntoExpr:
    """
    Removes all whitespace from a string.
    """
    expr = parse_into_expr(expr)
    return register_plugin(
        args=[expr],
        symbol="is_null_or_blank",
        is_elementwise=True,
        lib=lib,
        kwargs = {"all_white_space_as_null":all_white_space_as_null}
    )
