from typing import Any, Dict, List, OrderedDict

from polars import DataFrame, DataType, LazyFrame, Object, Unknown, col

from harley.utils import PolarsFrame


def column_to_list(df: PolarsFrame, column: str) -> List:
    """
    Given a column, returns the data as a list.

    ```python
    >>> import polars as pl
    >>> df = pl.DataFrame({
    ...     "a": [1, 2, 3],
    ...     "b": [4, 5, 6],
    ... })
    >>> harley.column_to_list(df, "a")
    [1, 2, 3]
    ```
    :param df: A polars DataFrame or LazyFrame
    :param column: A column name
    :return: A list of the data in the column
    """
    if isinstance(df := df.select(column), LazyFrame):
        df = df.collect()
    series = df.select(column).to_series()
    return series.to_list()


def nested_fields(schema: OrderedDict[str, DataType]) -> Dict[str, DataType]:
    """
    Returns only nested fields in a schema.
    
    ```python
    >>> import polars as pl
    >>> df = pl.DataFrame([
    ...:     {"id": 0, "students": ["Tao"]},
    ...:     {"id": 1, "students": ["Nicholas", "Charles"]}
    ...: ])
    >>> harley.nested_fields(df.schema)
    OrderedDict([('students', List(String))])
    ```
    :param schema: A schema
    :return: A dictionary of nested fields
    """
    return OrderedDict(
        [(field, d_type) for field, d_type in schema.items() if d_type.is_nested()]
    )


def two_columns_to_dictionary(
    df: DataFrame,
    key_col_name: str,
    value_col_name: str,
    allow_duplicates_keys: bool = False,
) -> Dict[Any, Any]:
    """
    Create a dictionary from two columns of a dataframe.

    ```python
    >>> import polars as pl
    >>> df = pl.DataFrame({
    ...     "a": [1, 2, 3],
    ...     "b": [4, 5, 6],
    ... })
    >>> harley.two_columns_to_dictionary(df, "a", "b")
    {1: 4, 2: 5, 3: 6}
    ```

    :param df: DataFrame
    :param key_col_name: column name of the key. The column should not be nested, nor should it be `Object` or `Unknown`.
    :param value_col_name: column name of the value
    :param allow_duplicates_keys: whether to allow duplicate keys. Defaults to `False`
    :return: dictionary from two columns
    """
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
