from polars import Expr
import polars as pl


def null_between(col: str, lower: str, upper: str) -> Expr:
    return (
        pl.when(pl.col(lower).is_null() & pl.col(upper).is_null())
        .then(False)
        .when(pl.col(col).is_null())
        .then(False)
        .when(
            pl.col(lower).is_null()
            & pl.col(upper).is_not_null()
            & (pl.col(col) <= pl.col(upper))
        )
        .then(True)
        .when(
            pl.col(upper).is_null()
            & pl.col(lower).is_not_null()
            & (pl.col(col) >= pl.col(lower))
        )
        .then(True)
        .otherwise(pl.col(col).is_between(pl.col(lower), pl.col(upper)))
    )
