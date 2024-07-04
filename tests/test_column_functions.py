from harley.column_functions import null_between
from tests.conftest import polars_frames
import pytest
from polars import DataType, DataFrame, LazyFrame
from polars.testing import assert_frame_equal
from typing import Tuple, Any
from harley.column_functions import multi_equals

@pytest.mark.parametrize("frame_type", polars_frames)
@pytest.mark.parametrize(
    "inp, exp",
    (
        (({"lower_age":[17], "upper_age":[None], "age":[94]}), True),
        (({"lower_age":[17],"upper_age": [None], "age":[10]}), False),
        (({"lower_age":[None],"upper_age": [10], "age":[5]}), True),
        (({"lower_age":[None],"upper_age": [10], "age":[88]}), False),
        (({"lower_age":[10], "upper_age":[15], "age":[11]}), True),
        (({"lower_age":[None], "upper_age":[None],"age": [11]}), False),
        (({"lower_age":[3], "upper_age":[5], "age":[None]}), False),
        (({"lower_age":[3], "upper_age":[3], "age":[None]}), False),
        (({"lower_age":[3], "upper_age":[3], "age":[3]}), True),
        (({"lower_age":[3], "upper_age":[4], "age":[5]}), False),
        (({"lower_age":[None], "upper_age":[None], "age":[None]}), False),
    ),
)
def test_null_between(frame_type: DataType, inp: Tuple[Any], exp: bool):
    df = frame_type(inp)
    exp = DataFrame({"res": [exp]})
    if isinstance(
        res := df.select(
            res=null_between(col="age", lower="lower_age", upper="upper_age")
        ),
        LazyFrame,
    ):
        res = res.collect()
    assert_frame_equal(exp, res)


@pytest.mark.parametrize("frame_type", polars_frames)
def test_multi_equals(frame_type:DataType):
    data = frame_type([{"col_1": "cat", "col_2": "cat"}, {"col_1": "cat", "col_2": "dog"}, {"col_1": "dog", "col_2": "cat"}, {"col_1": "dog", "col_2": "dog"}])
    exp = DataFrame({"res": [True, False, False, False]})
    if isinstance(res := data.select(res = multi_equals(cols=["col_1", "col_2"],val="cat")), LazyFrame):
        res = res.collect()
    assert_frame_equal(res, exp)
