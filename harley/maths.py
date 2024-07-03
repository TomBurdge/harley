from __future__ import annotations

from pathlib import Path
from typing import TYPE_CHECKING, Union

import polars as pl

from harley.utils import parse_into_expr, register_plugin, parse_version

if TYPE_CHECKING:
    from polars.type_aliases import IntoExpr

if parse_version(pl.__version__) < parse_version("0.20.16"):
    from polars.utils.udfs import _get_shared_lib_location

    lib: str | Path = _get_shared_lib_location(__file__)
else:
    lib = Path(__file__).parent


def div_or_else(dividend: IntoExpr,divisor:IntoExpr, or_else:Union[int, float] = 0.0) -> IntoExpr:
    """
    Return result of division of cola by colb or default if colb is zero.
    """
    dividend = parse_into_expr(dividend)
    divisor = parse_into_expr(divisor)
    return register_plugin(
        args=[dividend,divisor],
        symbol="div_or_else",
        is_elementwise=True,
        kwargs={"or_else":float(or_else)},
        lib=lib,
    )