from harley.utils import PolarsFrame
from .harley import columns_to_snake_case
from typing import Union, List, Dict
from polars import col, Struct, Expr, LazyFrame

import logging


def snake_case_column_names(df: PolarsFrame) -> PolarsFrame:
    new_col_names = columns_to_snake_case(df.columns)
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
    if isinstance(struct_columns, str):
        struct_columns = [struct_columns]
    if not recursive:
        limit = 1
    if limit is not None and not isinstance(limit, int):
        raise ValueError("limit must be a positive integer or None")
    if limit is not None and limit < 0:
        raise ValueError("limit must be a positive integer or None")
    if limit == 0:
        logging.warning("limit of 0 will result in no transformations")
        return df
    ldf = df.lazy() # noop if df is LazyFrame
    all_column_names = ldf.collect_schema().names()
    if any(separator in (witness := column) for column in all_column_names):
        logging.warning(
            f"separator \"{separator}\" found in column names, e.g. \"{witness}\". "
            "This might columns to be repeated this flatten_struct function to error"
        )
    non_struct_columns = list(set(ldf.collect_schema().names()) - set(struct_columns))
    struct_schema = ldf.select(*struct_columns).collect_schema()
    col_dtype_expr_names = [(struct_schema[c], col(c), c) for c in struct_columns]
    result_names: Dict[str, Expr] = {}
    level = 0
    while (limit is None and col_dtype_expr_names) or (limit is not None and level < limit):
        level += 1
        new_col_dtype_exprs = []
        for dtype, col_expr, name in col_dtype_expr_names:
            if not isinstance(dtype, Struct):
                if drop_original_struct:
                    if name in result_names:
                        raise ColumnNameRepeatedError(
                            f"Column name {name} would be repeated after flatten_struct"
                        )
                    result_names[name] = col_expr
                continue
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
    if level == limit and col_dtype_expr_names:
        for _, col_expr, name in col_dtype_expr_names:
            result_names[name] = col_expr
    if result_names and drop_original_struct:
        ldf = ldf.select(
            [col(c) for c in non_struct_columns] +
            [col_expr.alias(name) for name, col_expr in result_names.items()]
        )

    if isinstance(df, LazyFrame):
        return ldf
    return ldf.collect()
