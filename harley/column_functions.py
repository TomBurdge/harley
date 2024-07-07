from polars import Expr
import polars as pl
from typing import Any, List


def null_between(col: str, lower: str, upper: str) -> Expr:
    return (
        pl.when(pl.col(lower).is_null() & pl.col(upper).is_null())
        .then(False)
        .when(pl.col(col).is_null())
        .then(False)
        .when(pl.col(lower).is_null())
        .then(pl.col(col) <= pl.col(upper))
        .when(pl.col(upper).is_null())
        .then(pl.col(col) >= pl.col(lower))
        .otherwise(pl.col(col).is_between(pl.col(lower), pl.col(upper)))
    )


def multi_equals(cols: List[str], val: Any) -> Expr:
    query = [pl.col(name) == val for name in cols]
    return pl.all_horizontal(query)


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


def approx_equal(
    col_1: IntoExpr, col_2: IntoExpr, threshold: Union[float, int]
) -> IntoExpr:
    """
    Returns True if between
    """
    col_1 = parse_into_expr(col_1)
    col_2 = parse_into_expr(col_2)
    return register_plugin(
        args=[col_1, col_2],
        symbol="approx_equal",
        is_elementwise=True,
        lib=lib,
        kwargs={"threshold": float(threshold)},
    )
