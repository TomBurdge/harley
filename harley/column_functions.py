from __future__ import annotations

from pathlib import Path
from typing import TYPE_CHECKING, Any, List, Union

import polars as pl
from polars import Expr

from harley.utils import parse_into_expr, parse_version, register_plugin


def null_between(col: str, lower: str, upper: str) -> Expr:
    """Check if a column value is between two other column values.

    ```python
    >>> df = pl.DataFrame({
    ...         "col":   [1, 5, 3,    None],
    ...         "lower": [1, 2, None, None],
    ...         "upper": [2, 3, 3,    None]
    ...     })
    >>> df.select(res=harley.null_between("col", "lower", "upper"))
    shape: (4, 1)
    ┌───────┐
    │ res   │
    │ ---   │
    │ bool  │
    ╞═══════╡
    │ true  │
    │ false │
    │ true  │
    │ false │
    └───────┘
    ```

    :param col: name of the column to check
    :param lower: name of the column to use as lower bound
    :param upper: name of the column to use as upper bound
    :return: Boolean expression
    """
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
    """Check if multiple columns are equal to a value.

    ```python
    >>> df = pl.DataFrame({"col_1": [1, 2, 3], "col_2": [1, 3, 3]})
    >>> df.select(all_3=harley.multi_equals(["col_1", "col_2"], 3))
    shape: (3, 1)
    ┌───────┐
    │ all_3 │
    │ ---   │
    │ bool  │
    ╞═══════╡
    │ false │
    │ false │
    │ true  │
    └───────┘
    ```
    :param cols: list of column names
    :param val: value to compare
    :return: Boolean expression
    """
    query = [pl.col(name) == val for name in cols]
    return pl.all_horizontal(query)


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
    Returns whether the absolute difference between the values is less than or equal to the threshold.

    ```python
    >>> df = pl.DataFrame({"col_1": [1.0, 2.0, 3.0], "col_2": [1.9, 4.0, 1.5]})
    >>> df.select(close=harley.approx_equal("col_1", "col_2", 1.5))
    shape: (3, 1)
    ┌───────┐
    │ close │
    │ ---   │
    │ bool  │
    ╞═══════╡
    │ true  │
    │ false │
    │ true  │
    └───────┘
    ```
    :param col_1: column name
    :param col_2: column name
    :param threshold: threshold
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
