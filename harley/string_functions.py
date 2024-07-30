from __future__ import annotations

from pathlib import Path
from typing import TYPE_CHECKING, List

import polars as pl

from harley.utils import parse_into_expr, parse_version, register_plugin

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

    ```python
    >>> df = pl.DataFrame({"raw": ["  I\\r\\x0bgo   ", "\\tto", "school\\n", "by bus"]})
    >>> df.select(processed="'" + harley.single_space("raw") + "'")
    shape: (4, 1)
    ┌───────────┐
    │ processed │
    │ ---       │
    │ str       │
    ╞═══════════╡
    │ 'I go'    │
    │ 'to'      │
    │ 'school'  │
    │ 'by bus'  │
    └───────────┘
    ```
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

    ```python
    >>> df = pl.DataFrame({"raw": ["  I\\r\\x0bgo   ", "\\tto", "school\\n", "by bus"]})
    >>> df.select(processed="'" + harley.remove_all_whitespace("raw") + "'")
    shape: (4, 1)
    ┌───────────┐
    │ processed │
    │ ---       │
    │ str       │
    ╞═══════════╡
    │ 'Igo'     │
    │ 'to'      │
    │ 'school'  │
    │ 'bybus'   │
    └───────────┘
    ```
    """
    expr = parse_into_expr(expr)
    return register_plugin(
        args=[expr],
        symbol="remove_all_whitespace",
        is_elementwise=True,
        lib=lib,
    )


def remove_non_word_characters(expr: IntoExpr) -> IntoExpr:
    """
    Removes all non-word characters. "Word characters" are [\w\s], i.e. alphanumeric, whitespace, and underscore ("_").

    ```python
    >>> df = pl.DataFrame({"dirty": ["unicorns✨🦄✨", "fast?", "!slow"]})
    >>> df.select(clean = harley.remove_non_word_characters("dirty"))
    shape: (4, 1)
    ┌──────────┐
    │ clean    │
    │ ---      │
    │ str      │
    ╞══════════╡
    │ unicorns │
    │ fast     │
    │ slow     │
    └──────────┘
    ```
    """
    expr = parse_into_expr(expr)
    return register_plugin(
        args=[expr],
        symbol="remove_non_word_characters",
        is_elementwise=True,
        lib=lib,
    )


def anti_trim(expr: List[IntoExpr]) -> IntoExpr:
    """
    Replaces all whitespace between words, keeping leading and trailing spaces.

    ```python
    >>> df = pl.DataFrame({"raw": [" I  go ", "  to", "school", "by\\r\\t\\n\\vbus"]})
    >>> df.select(processed="'" + harley.anti_trim("raw") + "'")
    shape: (4, 1)
    ┌───────────┐
    │ processed │
    │ ---       │
    │ str       │
    ╞═══════════╡
    │ ' Igo '   │
    │ '  to'    │
    │ 'school'  │
    │ 'bybus'   │
    └───────────┘
    ```
    """
    expr = parse_into_expr(expr)
    return register_plugin(
        args=[expr],
        symbol="anti_trim",
        is_elementwise=True,
        lib=lib,
    )
