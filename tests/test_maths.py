from harley.maths import div_or_else
import pytest
from tests.conftest import polars_frames
from polars import DataFrame, DataType, LazyFrame
from polars.testing import assert_frame_equal
from typing import List, Dict, Any


@pytest.mark.parametrize("frame_type", polars_frames)
@pytest.mark.parametrize(
    "data, exp",
    (
        ({"dividend": [1, 2, 3, None], "divisor": [1, 1, 0, None]}, [1, 2, 0, None]),
        (
            {"dividend": [1.5, 2.5, 3, None], "divisor": [1, 1, 0, None]},
            [1.5, 2.5, 0, None],
        ),
        (
            {"dividend": [3, 0.5, 3, None], "divisor": [1.5, 0.5, 0.0, None]},
            [2.0, 1.0, 0.0, None],
        ),
    ),
)
def test_div_or_else(frame_type: DataType, data: Dict[str, List[Any]], exp: List):
    inp = frame_type(data)
    exp = DataFrame({"res": exp})
    if isinstance(
        res := inp.select(res=div_or_else(dividend="dividend", divisor="divisor")),
        LazyFrame,
    ):
        res = res.collect()
    assert_frame_equal(res, exp)
