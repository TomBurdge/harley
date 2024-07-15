from harley import column_to_list
from harley.dataframe_helper import nested_fields
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
    LazyFrame,
)
import pytest
from datetime import datetime
from harley.utils import polars_frames
from typing import OrderedDict, Union
from harley.dataframe_helper import two_columns_to_dictionary
from polars import DataFrame

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
def test_column_to_list(frame_type: Union[DataFrame, LazyFrame]):
    frame = frame_type(data)
    exp = floats
    res = column_to_list(frame, "float")
    assert exp == res


def test_nested_fields():
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
    res = nested_fields(schema=schema)
    exp = OrderedDict([("col_0", Array), ("col_1", List), ("col_2", Struct)])
    assert res == exp


def test_two_columns_to_dictionary_passes():
    inp_col_key_vals = [i for i in range(9)]
    inp_col_key = "inp_col_key"
    inp_col_val_vals = [i for i in range(1, 10)]
    inp_col_val = "inp_col_val"
    df = DataFrame({inp_col_key: inp_col_key_vals, inp_col_val: inp_col_val_vals})
    exp = dict(zip(inp_col_key_vals, inp_col_val_vals))
    res = two_columns_to_dictionary(
        df=df, key_col_name=inp_col_key, value_col_name=inp_col_val
    )
    assert res == exp


def test_two_columns_to_dictionary_raises_error_on_nested_keys():
    inp_col_key_vals = [[123], [234]]
    inp_col_key = "inp_col_key"
    inp_col_val_vals = [i for i in range(2)]
    inp_col_val = "inp_col_val"
    df = DataFrame({inp_col_key: inp_col_key_vals, inp_col_val: inp_col_val_vals})
    with pytest.raises(ValueError):
        two_columns_to_dictionary(
            df=df, key_col_name=inp_col_key, value_col_name=inp_col_val
        )


@pytest.fixture()
def duplicate_keys_df() -> DataFrame:
    inp_col_key_vals = ["duplicate", "duplicate", "not duplicate"]
    inp_col_key = "inp_col_key"
    inp_col_val_vals = [i for i in range(3)]
    inp_col_val = "inp_col_val"
    return DataFrame({inp_col_key: inp_col_key_vals, inp_col_val: inp_col_val_vals})


def test_two_columns_to_dictionary_raises_error_on_duplicate_keys_by_default(
    duplicate_keys_df: DataFrame,
):
    with pytest.raises(ValueError):
        two_columns_to_dictionary(
            df=duplicate_keys_df,
            key_col_name="inp_col_key",
            value_col_name="inp_col_val",
            allow_duplicates_keys=False,
        )


def test_two_columns_to_dictionary_allows_duplicate_keys(duplicate_keys_df: DataFrame):
    exp = {"duplicate": 1, "not duplicate": 2}
    res = two_columns_to_dictionary(
        df=duplicate_keys_df,
        key_col_name="inp_col_key",
        value_col_name="inp_col_val",
        allow_duplicates_keys=True,
    )
    assert res == exp
