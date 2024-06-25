from harley.utils import PolarsFrame
from .harley import columns_to_snake_case


def snake_case_column_names(df: PolarsFrame) -> PolarsFrame:
    new_col_names = columns_to_snake_case(df.columns)
    return df.rename(new_col_names)
