import pytest
from tests.conftest import polars_frames
from harley.column_functions import multi_equals
from polars import DataFrame, LazyFrame, DataType
from polars.testing import assert_frame_equal

@pytest.mark.parametrize("frame_type", polars_frames)
def test_multi_equals(frame_type:DataType):
    data = frame_type([{"col_1": "cat", "col_2": "cat"}, {"col_1": "cat", "col_2": "dog"}, {"col_1": "dog", "col_2": "cat"}, {"col_1": "dog", "col_2": "dog"}])
    exp = DataFrame({"res": [True, False, False, False]})
    if isinstance(res := data.select(res = multi_equals(cols=["col_1", "col_2"],val="cat")), LazyFrame):
        res = res.collect()
    assert_frame_equal(res, exp)