from harley.utils import PolarsFrame
from .harley import columns_to_snake_case
from typing import Union, OrderedDict, List
from polars import col


def snake_case_column_names(df: PolarsFrame) -> PolarsFrame:
    new_col_names = columns_to_snake_case(df.columns)
    return df.rename(new_col_names)


def flatten_struct(
    df: PolarsFrame, struct_columns: Union[str, List[str]], separator: str = ":"
) -> PolarsFrame:
    if isinstance(struct_columns, str):
        struct_columns = [struct_columns]
    struct_schema = df.select(*struct_columns).schema
    unnested_columns = []
    for struct_col in struct_columns:
        nested_columns = struct_schema[struct_col]
        unnested_columns += [
            col(struct_col)
            .struct.field(field)
            .alias(separator.join([struct_col, field]))
            for field, _ in nested_columns
        ]
    return df.with_columns(unnested_columns).drop(struct_columns)
