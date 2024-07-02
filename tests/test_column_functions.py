from harley.column_functions import null_between
from tests.conftest import polars_frames
import pytest
from polars import DataType, DataFrame, LazyFrame
from polars.testing import assert_frame_equal
from typing import Tuple, Any


@pytest.mark.parametrize("frame_type", polars_frames)
@pytest.mark.parametrize(
    "inp, exp",
    (
        (([17], [None], [94]), True),
        (([17], [None], [10]), False),
        (([None], [10], [5]), True),
        (([None], [10], [88]), False),
        (([10], [15], [11]), True),
        (([None], [None], [11]), False),
        (([3], [5], [None]), False),
        (([None], [None], [None]), False),
    ),
)
def test_null_between(frame_type: DataType, inp: Tuple[Any], exp: bool):
    schema = ["lower_age", "upper_age", "age"]
    data = dict(zip(schema, inp))
    df = frame_type(data)
    exp = DataFrame({"res": [exp]})
    if isinstance(
        res := df.select(
            res=null_between(col="age", lower="lower_age", upper="upper_age")
        ),
        LazyFrame,
    ):
        res = res.collect()
    assert_frame_equal(exp, res)
