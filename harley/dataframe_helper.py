from harley.utils import PolarsFrame
from polars import LazyFrame
from typing import List, Dict
import polars.datatypes as T
from polars import DataType
from typing import OrderedDict, Tuple


def column_to_list(df: PolarsFrame, column: str) -> List:
    if isinstance(df := df.select(column), LazyFrame):
        df = df.collect()
    series = df.select(column).to_series()
    return series.to_list()


def nested_fields(schema: OrderedDict[str, DataType]) -> Dict[str, DataType]:
    """
    Returns only nested fields in a schema.
    Where 'complex' means nested.
    """
    return OrderedDict(
        [
            (field, d_type)
            for field, d_type in schema
            if d_type.is_nested()
        ]
    )
