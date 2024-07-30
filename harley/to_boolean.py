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

    ```python
    >>> df = pl.DataFrame({"strings": ["", None, "foo"], "lists": [[], None, [3]]})
    >>> df.select(
    ...     str_check = harley.is_null_or_blank("strings"),
    ...     list_check = harley.is_null_or_blank("lists"),
    ... )
    shape: (3, 2)
    ┌───────────┬────────────┐
    │ str_check ┆ list_check │
    │ ---       ┆ ---        │
    │ bool      ┆ bool       │
    ╞═══════════╪════════════╡
    │ true      ┆ true       │
    │ true      ┆ true       │
    │ false     ┆ false      │
    └───────────┴────────────┘
    ```
    """
    if not all_white_space_as_null:
        expr = parse_into_expr(expr)
        return register_plugin(
            args=[expr],
            symbol="is_null_or_blank",
            is_elementwise=True,
            lib=lib,
        )
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

    ```python
    >>> df = pl.DataFrame({"bools": [True, False, None]})
    >>> df.select(res = harley.is_falsey("bools"))
    shape: (3, 1)
    ┌───────┐
    │ res   │
    │ ---   │
    │ bool  │
    ╞═══════╡
    │ false │
    │ true  │
    │ true  │
    └───────┘
    ```
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

    ```python
    >>> df = pl.DataFrame({"bools": [True, False, None]})
    >>> df.select(res = harley.is_false("bools"))
    shape: (3, 1)
    ┌───────┐
    │ res   │
    │ ---   │
    │ bool  │
    ╞═══════╡
    │ false │
    │ true  │
    │ false │
    └───────┘
    ```
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


    ```python
    >>> df = pl.DataFrame({"bools": [True, False, None]})
    >>> df.select(res = harley.is_truthy("bools"))
    shape: (3, 1)
    ┌───────┐
    │ res   │
    │ ---   │
    │ bool  │
    ╞═══════╡
    │ true  │
    │ false │
    │ true  │
    └───────┘
    ```
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

    ```python
    >>> df = pl.DataFrame({"bools": [True, False, None]})
    >>> df.select(res = harley.is_true("bools"))
    shape: (3, 1)
    ┌───────┐
    │ res   │
    │ ---   │
    │ bool  │
    ╞═══════╡
    │ true  │
    │ false │
    │ false │
    └───────┘
    ```
    """
    expr = parse_into_expr(expr)
    return register_plugin(
        args=[expr],
        symbol="is_true",
        is_elementwise=True,
        lib=lib,
    )
