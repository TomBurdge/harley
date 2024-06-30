from harley import column_to_list
import pytest
from datetime import datetime
from tests.conftest import polars_frames
from harley.dataframe_helper import two_columns_to_dictionary
from polars import DataFrame

floats=[4.0, 5.0, 6.0, 7.0, 8.0]
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
def test_column_to_list(frame_type:str):
    frame = frame_type(data)
    exp = floats
    res = column_to_list(frame, "float")
    assert exp==res


def test_two_columns_to_dictionary_passes():
    inp_col_key_vals = [i for i in range(9)]
    inp_col_key="inp_col_key"
    inp_col_val_vals = [i for i in range(1, 10)]
    inp_col_val="inp_col_val"
    df = DataFrame({inp_col_key:inp_col_key_vals, inp_col_val:inp_col_val_vals})
    exp = dict(zip(inp_col_key_vals, inp_col_val_vals))
    res = two_columns_to_dictionary(df = df, key_col_name=inp_col_key, value_col_name=inp_col_val)
    assert res==exp

def test_two_columns_to_dictionary_raises_error_on_nested_keys():
    inp_col_key_vals = [[123],[234]]
    inp_col_key="inp_col_key"
    inp_col_val_vals = [i for i in range( 2)]
    inp_col_val="inp_col_val"
    df = DataFrame({inp_col_key:inp_col_key_vals, inp_col_val:inp_col_val_vals})
    with pytest.raises(ValueError):
        two_columns_to_dictionary(df = df, key_col_name=inp_col_key, value_col_name=inp_col_val)