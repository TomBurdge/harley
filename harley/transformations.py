from harley.utils import PolarsFrame
from .harley import columns_to_snake_case
from typing import Union, List
from polars import col, Struct, LazyFrame


def snake_case_column_names(df: PolarsFrame) -> PolarsFrame:
    if isinstance(df, LazyFrame):
        all_column_names = df.collect_schema().names()
    else:
        all_column_names = df.columns
    new_col_names = columns_to_snake_case(all_column_names)
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
