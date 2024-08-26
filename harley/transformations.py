from harley.utils import PolarsFrame
from .harley import columns_to_snake_case  # noqa
from typing import Union, List, Dict
from polars import col, Struct, Expr, LazyFrame

import warnings


def snake_case_column_names(df: PolarsFrame) -> PolarsFrame:
    """
    Takes a PolarsFrame, converts its column names to snake case,
    and returns the modified PolarsFrame.
    
    :param df: A PolarsFrame object, which can be eith er a LazyFrame or DataFrame.
    :type df: PolarsFrame
    :return: The function `snake_case_column_names` is returning a PolarsFrame with column names
    converted to snake case.
    """
    if isinstance(df, LazyFrame):
        all_column_names = df.collect_schema().names()
    else:
        all_column_names = df.columns
    new_col_names = columns_to_snake_case(all_column_names)
    return df.rename(new_col_names)


class ColumnNameRepeatedError(ValueError):
    """raised when a column name would be repeated after flatten_struct"""


def flatten_struct(
    df: PolarsFrame,
    struct_columns: Union[str, List[str]],
    separator: str = ":",
    drop_original_struct: bool = True,
    recursive: bool = False,
    limit: int = None,
) -> PolarsFrame:
    """
    Takes a PolarsFrame and flattens specified struct columns into
    separate columns using a specified separator,
    with options to control recursion and limit the number
    of flattening levels.

    :param df: A PolarsFrame, either a LazyFrame or DataFrame.
    :type df: PolarsFrame
    :param struct_columns: The column or columns in the PolarsFrame that contain struct data.
    This function is designed to flatten the struct data into separate columns based on the fields within the struct.
    :type struct_columns: Union[str, List[str]]
    :param separator: Specifies the character or string that will be used to separate the original
    column name from the nested field names when flattening a nested struct column.
    :type separator: str (optional)
    :param drop_original_struct: Determines whether the original struct columns should be dropped after flattening or not,
    defaults to True.
    :type drop_original_struct: bool (optional)
    :param recursive: Determines whether the flattening process should be applied recursively to
    all levels of nested structures within the specified struct columns, defaults to False.
    :type recursive: bool (optional)
    :param limit: Determines the maximum number of levels to flatten the struct columns.
    If `limit` is set to a positive integer, the function will flatten the struct columns up to that specified level.
    If `limit` is set to `None`, there is no limit.
    :type limit: int
    :return: returns a PolarsFrame.
    """
    if isinstance(struct_columns, str):
        struct_columns = [struct_columns]
    if not recursive:
        limit = 1
    if limit is not None and not isinstance(limit, int):
        raise ValueError("limit must be a positive integer or None")
    if limit is not None and limit < 0:
        raise ValueError("limit must be a positive integer or None")
    if limit == 0:
        warnings.warn("limit of 0 will result in no transformations")
        return df
    ldf = df.lazy()  # noop if df is LazyFrame
    all_column_names = ldf.collect_schema().names()
    if any(separator in (witness := column) for column in all_column_names):
        warnings.warn(
            f'separator "{separator}" found in column names, e.g. "{witness}". '
            "If columns would be repeated, this function will error"
        )
    non_struct_columns = list(set(ldf.collect_schema().names()) - set(struct_columns))
    struct_schema = ldf.select(*struct_columns).collect_schema()
    col_dtype_expr_names = [(struct_schema[c], col(c), c) for c in struct_columns]
    result_names: Dict[str, Expr] = {}
    level = 0
    while (limit is None and col_dtype_expr_names) or (
        limit is not None and level < limit
    ):
        level += 1
        new_col_dtype_exprs = []
        for dtype, col_expr, name in col_dtype_expr_names:
            if not isinstance(dtype, Struct):
                if name in result_names:
                    raise ColumnNameRepeatedError(
                        f"Column name {name} would be created at least twice after flatten_struct"
                    )
                result_names[name] = col_expr
                continue
            if any(separator in (witness := field.name) for field in dtype.fields):
                warnings.warn(
                    f'separator "{separator}" found in field names, e.g. "{witness}" in {name}. '
                    "If columns would be repeated, this function will error"
                )
            new_col_dtype_exprs += [
                (
                    field.dtype,
                    col_expr.struct.field(field.name),
                    name + separator + field.name,
                )
                for field in dtype.fields
            ]
            if not drop_original_struct:
                ldf = ldf.with_columns(
                    col_expr.struct.field(field.name).alias(
                        name + separator + field.name
                    )
                    for field in dtype.fields
                )
        col_dtype_expr_names = new_col_dtype_exprs
    if drop_original_struct and level == limit and col_dtype_expr_names:
        for _, col_expr, name in col_dtype_expr_names:
            result_names[name] = col_expr
    if any((witness := column) in non_struct_columns for column in result_names):
        raise ColumnNameRepeatedError(
            f"Column name {witness} would be created after flatten_struct, but it's already a non-struct column"
        )
    if drop_original_struct:
        ldf = ldf.select(
            [col(c) for c in non_struct_columns]
            + [col_expr.alias(name) for name, col_expr in result_names.items()]
        )

    if isinstance(df, LazyFrame):
        return ldf
    return ldf.collect()
