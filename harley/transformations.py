from harley.utils import PolarsFrame
from .harley import columns_to_snake_case
from typing import Union, List, Dict
from polars import col, Struct, Expr, LazyFrame

import logging


def snake_case_column_names(df: PolarsFrame) -> PolarsFrame:
    new_col_names = columns_to_snake_case(df.columns)
    return df.rename(new_col_names)


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
    if not isinstance(limit, int) and limit is not None:
        raise ValueError("limit must be a positive integer or None")
    if limit is not None and limit < 0:
        raise ValueError("limit must be a positive integer or None")
    if limit == 0:
        logging.warning("limit of 0 will result in no transformations")
        return df
    if not isinstance(ldf := df, LazyFrame):
        ldf = ldf.lazy()

    non_struct_columns = list(set(ldf.collect_schema().names()) - set(struct_columns))
    struct_schema = ldf.select(*struct_columns).collect_schema()
    col_dtype_expr_names = [(struct_schema[c], col(c), c) for c in struct_columns]
    result_names: Dict[Tuple[int, str], Expr] = {}
    level = 0
    while (limit is None and col_dtype_expr_names) or (limit is not None and level < limit):
        level += 1
        new_col_dtype_exprs = []
        for dtype, col_expr, name in col_dtype_expr_names:
            if not isinstance(dtype, Struct):
                if drop_original_struct:
                    result_names[(level, name)] = col_expr
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
        for dtype, col_expr, name in col_dtype_expr_names:
            result_names[(level, name)] = col_expr
    if result_names and drop_original_struct:
        ldf = ldf.select(
            [col(c) for c in non_struct_columns] +
            [col_expr.alias(name) for (_, name), col_expr in result_names.items()]
        )

    if isinstance(df, LazyFrame):
        return ldf
    return ldf.collect()


"""
    struct_schema = df.select(*struct_columns).schema
    unnested_columns = []
    new_col_names = []
    for struct_col in struct_columns:
        nested_columns = struct_schema[struct_col]
        unnested_columns += [
            col(struct_col)
            .struct.field(field)
            .alias(separator.join([struct_col, field]))
            for field, _ in nested_columns
        ]
        if recursive:
            new_col_names += [
                separator.join([struct_col, field]) for field, _ in nested_columns
            ]
    df = df.with_columns(unnested_columns)
    if drop_original_struct:
        df = df.drop(struct_columns)
    if recursive and not limit == 0:
        # empty list is Falsey
        if (new_unnested_columns := [
            col
            for col, dtype in df.select(new_col_names).schema.items()
            if isinstance(dtype, Struct)
        ]):
            df = flatten_struct(
                df=df,
                struct_columns=new_unnested_columns,
                separator=separator,
                drop_original_struct=drop_original_struct,
                recursive=True,
                limit=(limit - 1 if limit else None),
            )
    return df
"""
