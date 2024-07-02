from polars import Expr
from typing import Any, List
import polars as pl

def multi_equals(cols: List[str], val: Any) -> Expr:
    query = [pl.col(name) == val for name in cols]
    return pl.all_horizontal(query)