from harley import column_to_list
from harley.dataframe_helper import complex_fields
from polars import (
    Decimal,
    Float32,
    Float64,
    Int8,
    Int16,
    Int32,
    Int64,
    UInt8,
    UInt32,
    UInt64,
    Date,
    Datetime,
    Duration,
    Time,
    Array,
    List,
    Struct,
    String,
    Categorical,
    Enum,
    Utf8,
    Binary,
    Boolean,
    Null,
    Object,
    Unknown,
)
import pytest
from datetime import datetime
from tests.conftest import polars_frames
from typing import OrderedDict

floats = [4.0, 5.0, 6.0, 7.0, 8.0]
data = {
    "integer": [1, 2, 3, 4, 5],
    "date": [
        datetime(2022, 1, 1),
        datetime(2022, 1, 2),
        datetime(2022, 1, 3),
        datetime(2022, 1, 4),
        datetime(2022, 1, 5),
    ],
    "float": floats,
}


@pytest.mark.parametrize("frame_type", polars_frames)
def test_column_to_list(frame_type: str):
    frame = frame_type(data)
    exp = floats
    res = column_to_list(frame, "float")
    assert exp == res


def test_complex_fields():
    all_column_types = [
        Array,
        List,
        Struct,
        Decimal,
        Float32,
        Float64,
        Int8,
        Int16,
        Int32,
        Int64,
        UInt8,
        UInt32,
        UInt64,
        Date,
        Datetime,
        Duration,
        Time,
        String,
        Categorical,
        Enum,
        Utf8,
        Binary,
        Boolean,
        Null,
        Object,
        Unknown,
    ]
    schema = [
        ("col_" + str(i), col_type) for i, col_type in enumerate(all_column_types)
    ]
    res = complex_fields(schema=schema)
    exp = OrderedDict([("col_0", Array), ("col_1", List), ("col_2", Struct)])
    assert res == exp
