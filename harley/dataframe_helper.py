from harley.utils import PolarsFrame
from polars import LazyFrame, Array, Struct, DataFrame
from polars import List as PolarsList
from typing import List, Dict, Any


def column_to_list(df: PolarsFrame, column: str) -> List:
    if isinstance(df := df.select(column), LazyFrame):
        df = df.collect()
    series = df.select(column).to_series()
    return series.to_list()


def two_columns_to_dictionary(
    df: DataFrame, key_col_name: str, value_col_name: str
) -> Dict[Any, Any]:
    df = df.select(key_col_name, value_col_name)
    if df.dtypes[0] in [Array, PolarsList, Struct]:
        raise ValueError(
            f"Column {key_col_name} is of type {df.dtypes[0]} will not return a hashable type.",
            f"Therefore {key_col_name} cannot provide an acceptable key",
        )
    df = df.to_dicts()
    return {row[key_col_name]: row[value_col_name] for row in df}