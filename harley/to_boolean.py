from __future__ import annotations

from pathlib import Path
from typing import TYPE_CHECKING

import polars as pl

from harley.utils import parse_into_expr, parse_version, register_plugin

if TYPE_CHECKING:
    from polars.type_aliases import IntoExpr
if parse_version(pl.__version__) < parse_version("0.20.16"):
    from polars.utils.udfs import _get_shared_lib_location

    lib: str | Path = _get_shared_lib_location(__file__)
else:
    lib = Path(__file__).parent


def is_null_or_blank(expr: IntoExpr, all_white_space_as_null: bool = False) -> IntoExpr:
    """
    Returns True if null/blank/empty list.
    Otherwise, False.
    """
    expr = parse_into_expr(expr)
    return register_plugin(
        args=[expr],
        symbol="is_null_or_blank",
        is_elementwise=True,
        lib=lib,
        kwargs={"all_white_space_as_null": all_white_space_as_null},
    )


def is_falsey(expr: IntoExpr) -> IntoExpr:
    """
    Returns True if null/False, otherwise False.
    """
    expr = parse_into_expr(expr)
    return register_plugin(
        args=[expr],
        symbol="is_falsey",
        is_elementwise=True,
        lib=lib,
    )


def is_false(expr: IntoExpr) -> IntoExpr:
    """
    Returns True if False, otherwise False.
    Whitespace aware.
    """
    expr = parse_into_expr(expr)
    return register_plugin(
        args=[expr],
        symbol="is_false",
        is_elementwise=True,
        lib=lib,
    )


def is_truthy(expr: IntoExpr) -> IntoExpr:
    """
    Returns True if True, otherwise False.
    Whitespace aware.
    """
    expr = parse_into_expr(expr)
    return register_plugin(
        args=[expr],
        symbol="is_truthy",
        is_elementwise=True,
        lib=lib,
    )


def is_true(expr: IntoExpr) -> IntoExpr:
    """
    Returns True if True, otherwise False.
    Whitespace aware.
    """
    expr = parse_into_expr(expr)
    return register_plugin(
        args=[expr],
        symbol="is_true",
        is_elementwise=True,
        lib=lib,
    )
