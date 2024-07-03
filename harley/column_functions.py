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