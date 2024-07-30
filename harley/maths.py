from __future__ import annotations

from pathlib import Path
from typing import TYPE_CHECKING, Union

import polars as pl

from harley.utils import parse_into_expr, parse_version, register_plugin

if TYPE_CHECKING:
    from polars.type_aliases import IntoExpr

if parse_version(pl.__version__) < parse_version("0.20.16"):
    from polars.utils.udfs import _get_shared_lib_location

    lib: str | Path = _get_shared_lib_location(__file__)
else:
    lib = Path(__file__).parent


def div_or_else(
    dividend: IntoExpr, divisor: IntoExpr, or_else: Union[int, float] = 0.0
) -> IntoExpr:
    """
    Return result of division of dividend by divisor or `or_else` if divisor is zero.

    ```python
    >>> df = pl.DataFrame({"a": [1.3, 0, 3.8], "b": [5.2, -6.7, 0]})
    >>> df.select(q = harley.div_or_else("a", "b", 42))
    shape: (3, 1)
    ┌──────┐
    │ q    │
    │ ---  │
    │ f64  │
    ╞══════╡
    │ 0.25 │
    │ -0.0 │
    │ 42.0 │
    └──────┘
    ```

    :param dividend: dividend
    :param divisor: divisor
    :param or_else: or_else
    """
    dividend = parse_into_expr(dividend)
    divisor = parse_into_expr(divisor)
    return register_plugin(
        args=[dividend, divisor],
        symbol="div_or_else",
        is_elementwise=True,
        kwargs={"or_else": float(or_else)},
        lib=lib,
    )
