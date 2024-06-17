from harley.utils import PolarsFrame
from polars import LazyFrame
from typing import List


def column_to_list(df: PolarsFrame, column: str) -> List:
    if isinstance(df := df.select(column), LazyFrame):
        df = df.collect()
    series = df.select(column).to_series()
    return series.to_list()
