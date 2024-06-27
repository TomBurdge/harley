from __future__ import annotations

from pathlib import Path
from typing import TYPE_CHECKING

import polars as pl


from harley.utils import parse_into_expr, register_plugin, parse_version
from harley.dataframe_helper import column_to_list
from harley.string_functions import single_space, remove_all_whitespace

if TYPE_CHECKING:
    from polars.type_aliases import IntoExpr

if parse_version(pl.__version__) < parse_version("0.20.16"):
    from polars.utils.udfs import _get_shared_lib_location

    lib: str | Path = _get_shared_lib_location(__file__)
else:
    lib = Path(__file__).parent

__all__ = ["column_to_list", "single_space", "remove_all_whitespace"]
