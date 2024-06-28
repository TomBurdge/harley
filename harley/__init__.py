from __future__ import annotations

from pathlib import Path
from typing import TYPE_CHECKING

import polars as pl


from harley.utils import parse_into_expr, register_plugin, parse_version
from harley.dataframe_helper import column_to_list
<<<<<<< HEAD
from harley.transformations import snake_case_column_names
=======
>>>>>>> a0bd4aab6c083df0c9b81759f0998d5047f0b48a
from harley.string_functions import single_space, remove_all_whitespace, remove_non_word_characters

if TYPE_CHECKING:
    from polars.type_aliases import IntoExpr

if parse_version(pl.__version__) < parse_version("0.20.16"):
    from polars.utils.udfs import _get_shared_lib_location

    lib: str | Path = _get_shared_lib_location(__file__)
else:
    lib = Path(__file__).parent

__all__ = ["column_to_list", "single_space", "snake_case_column_names", "remove_all_whitespace", "remove_non_word_characters"]
