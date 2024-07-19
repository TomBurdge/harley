from __future__ import annotations
from pathlib import Path
from typing import TYPE_CHECKING, Optional
import polars as pl
from harley.utils import parse_into_expr, register_plugin, parse_version

if TYPE_CHECKING:
    from polars.type_aliases import IntoExpr
if parse_version(pl.__version__) < parse_version("0.20.16"):
    from polars.utils.udfs import _get_shared_lib_location

    lib: str | Path = _get_shared_lib_location(__file__)
else:
    lib = Path(__file__).parent

def array_choice(expr: IntoExpr, seed: Optional[int] = None) -> Expr:
    """Select a random element from an array column, with an optional seed

    Parameters
    ----------
    expr : Expr
        Expression to select from.
    seed : int, optional
        Seed for the random number generator.

    Returns
    -------
    Expr
    """
    expr = parse_into_expr(expr)
    return register_plugin(
        args=[expr],
        symbol="array_choice",
        is_elementwise=True,
        lib=lib,
        kwargs={"seed": seed},
    )