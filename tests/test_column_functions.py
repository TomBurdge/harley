from harley.column_functions import approx_equal
from tests.conftest import polars_frames
from polars import DataType, DataFrame, LazyFrame
import pytest
from typing import List, Union
from polars.testing import assert_frame_equal

@pytest.mark.parametrize("frame_type", polars_frames)
@pytest.mark.parametrize(
    "inp, exp, thresh",
    (
        (
            [[1.1, 1.1, 1.02, 1.02], [1.05, 11.6, 1.09, 1.34]],
            [True, False, True, False],
            0.1,
        ),
        (
            [[12, 20, 44, 32], [14, 26, 41, 9]],
            [True, False, True, False],
            5,
        ),
        (
            [[12, 20, 44, 32], [14, 26, 41, 9]],
            [True, False, True, False],
            4.1,
        ),
            (
            [[12, 20, 44, 32], [14, 26, 41, 9]],
            [True, False, True, False],
            3.9,
        ),
    ),
)
def test_approx_equal(
    frame_type: DataType,
    inp: List[List[Union[float, int]]],
    exp: List[bool],
    thresh: float,
):
    schema = ["left", "right"]
    data = frame_type(dict(zip(schema, inp)))
    exp = DataFrame({"res":exp})
    if isinstance(res := data.select(res = approx_equal(col_1 ="left", col_2="right", threshold=thresh)), LazyFrame):
        res = res.collect()
    assert_frame_equal(res, exp)
