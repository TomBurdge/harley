import pytest
from tests.conftest import polars_frames
import polars as pl
from harley.to_boolean import is_null_or_blank
from polars import DataFrame, DataType, LazyFrame
from polars.testing import assert_frame_equal
from typing import List

@pytest.mark.parametrize("frame_type, inp, exp, all_white_space_as_null",     (
    (pl.DataFrame, [" ", "", "not blank", None], [False, True, False, True],False),
    (pl.DataFrame, [" ", "", "not blank", None],[True, True, False, True],True),
    (pl.DataFrame, [[], [123], None],[True, False, True], False)
    ))
def test_is_null_or_blank(frame_type:DataType, inp:List, exp:List,all_white_space_as_null:bool):
    data = frame_type({"value":inp})
    if isinstance(res := data.select(res = is_null_or_blank("value", all_white_space_as_null)), LazyFrame):
        res = res.collect()
    exp = DataFrame({"res":exp})
    assert_frame_equal(res, exp)
