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


def div_or_else(
    dividend: IntoExpr, divisor: IntoExpr, or_else: Union[int, float] = 0.0
) -> IntoExpr:
    """
    Returns the result of dividing one expression by another, with an optional default
    value if the divisor is zero.

    :param dividend: The value that will be divided
    :type dividend: IntoExpr
    :param divisor: The value by which
    the `dividend` will be divided. If the `divisor` is zero, the function will return the `or_else`
    value instead of performing the division
    :type divisor: IntoExpr
    :param or_else: TA default value that will
    be returned if the divisor is zero. It is a numeric value (either an integer or a float) and is set
    to 0.0 by default if not provided explicitly.
    :type or_else: Union[int, float]
    :return: The result of the division of `dividend` by `divisor`,
    or the default value `or_else` if the divisor is zero.
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
