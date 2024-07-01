from harley.utils import PolarsFrame
from polars import LazyFrame, Object, Unknown, DataFrame
from polars import col
from typing import List, Dict, Any


def column_to_list(df: PolarsFrame, column: str) -> List:
    if isinstance(df := df.select(column), LazyFrame):
        df = df.collect()
    series = df.select(column).to_series()
    return series.to_list()


def two_columns_to_dictionary(
    df: DataFrame,
    key_col_name: str,
    value_col_name: str,
    allow_duplicates_keys: bool = False,
) -> Dict[Any, Any]:
    df = df.select(key_col_name, value_col_name)
    key_dtype = df.dtypes[0]
    if key_dtype.is_nested():
        raise ValueError(
            f"Column {key_col_name} is of type {df.dtypes[0]} will not return a hashable type.",
            f"Therefore {key_col_name} cannot provide an acceptable key",
        )
    if key_dtype in [Object, Unknown]:
        raise ValueError(
            f"Column {key_col_name} is of type {df.dtypes[0]} and may not return a hashable type.",
            f"Therefore {key_col_name} cannot provide an acceptable key.",
        )
    if not allow_duplicates_keys:
        if df.filter(col(key_col_name).is_duplicated()).height > 0:
            raise ValueError(
                "Duplicate records found in key column.",
                "Duplicate keys will be overwritten and de-duplicated unpredicatbly.",
                "To allow duplicate keys, set `allow_duplicate_keys` to True",
                "Some of the duplicates:",
                column_to_list(
                    df.filter((col(key_col_name).is_duplicated())), key_col_name
                )[:3],
            )
    df = df.to_dicts()
    return {row[key_col_name]: row[value_col_name] for row in df}
