from polars import DataFrame, LazyFrame, DataType
from typing import List, Any

polars_frames = [DataFrame, LazyFrame]

def combine_pytest_params_with_dtypes(dtypes: List[DataType], parameters: List[List[Any]]) -> List[List[Any]]:
    dtype_column_combinations = []
    for params in parameters:
        for dtype in dtypes:
            dtype_column_combinations.append(tuple([dtype] + params))
    return dtype_column_combinations
