from harley.maths import div_or_else
import pytest
from harley.utils import polars_frames
from polars import DataFrame, LazyFrame
from polars.testing import assert_frame_equal
from typing import List, Dict, Any, Union


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
            {"dividend": [0.5, 3, 3, None], "divisor": [0.5, 1.5, 0.0, None]},
            [1.0, 2.0, 0.0, None],
        ),
    ),
)
def test_div_or_else(
    frame_type: Union[DataFrame, LazyFrame], data: Dict[str, List[Any]], exp: List
):
    inp = frame_type(data)
    exp = DataFrame({"res": exp})
    if isinstance(
        res := inp.select(res=div_or_else(dividend="dividend", divisor="divisor")),
        LazyFrame,
    ):
        res = res.collect()
    assert_frame_equal(res, exp)


@pytest.mark.parametrize("frame_type", polars_frames)
def test_div_or_else_alt_or_else(frame_type: Union[DataFrame, LazyFrame]):
    inp = frame_type({"dividend": [1.5, 2.5, 3, None], "divisor": [1, 1, 0, None]})
    exp = DataFrame({"res": [1.5, 2.5, 5, None]})
    if isinstance(
        res := inp.select(
            res=div_or_else(dividend="dividend", divisor="divisor", or_else=5)
        ),
        LazyFrame,
    ):
        res = res.collect()
    assert_frame_equal(res, exp)
