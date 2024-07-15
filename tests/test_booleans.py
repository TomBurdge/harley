import pytest
from harley.utils import polars_frames
from harley.to_boolean import is_null_or_blank, is_true, is_truthy, is_false, is_falsey
from polars import DataFrame, LazyFrame
from polars.testing import assert_frame_equal
from typing import List, Callable, Union



@pytest.mark.parametrize("frame_type", polars_frames)
@pytest.mark.parametrize(
    "inp, exp, all_white_space_as_null",
    [
        [[" ", "", "not blank", None], [False, True, False, True], False],
        [[" ", "", "not blank", None], [True, True, False, True], True],
        [[[], [123], None], [True, False, True], False],
    ],
)
def test_is_null_or_blank(
    frame_type: Union[DataFrame, LazyFrame],
    inp: List,
    exp: List,
    all_white_space_as_null: bool,
):
    data = frame_type({"value": inp})
    if isinstance(
        res := data.select(res=is_null_or_blank("value", all_white_space_as_null)),
        LazyFrame,
    ):
        res = res.collect()
    exp = DataFrame({"res": exp})
    assert_frame_equal(res, exp)


@pytest.mark.parametrize("frame_type", polars_frames)
@pytest.mark.parametrize("tested_function", [is_truthy, is_true])
def test_is_true_truthy(
    frame_type: Union[DataFrame, LazyFrame], tested_function: Callable
):
    inp = [True, False, None]
    exp = DataFrame({"res": [True, False, False]})
    data = frame_type({"value": inp})
    if isinstance(res := data.select(res=tested_function("value")), LazyFrame):
        res = res.collect()
    assert_frame_equal(res, exp)


@pytest.mark.parametrize("frame_type", polars_frames)
def test_is_false(frame_type: Union[DataFrame, LazyFrame]):
    inp = [True, False, None]
    exp = DataFrame({"res": [False, True, False]})
    data = frame_type({"value": inp})
    if isinstance(res := data.select(res=is_false("value")), LazyFrame):
        res = res.collect()
    assert_frame_equal(res, exp)


@pytest.mark.parametrize("frame_type", polars_frames)
def test_is_falsey(frame_type: Union[DataFrame, LazyFrame]):
    inp = [True, False, None]
    exp = DataFrame({"res": [False, True, True]})
    data = frame_type({"value": inp})
    if isinstance(res := data.select(res=is_falsey("value")), LazyFrame):
        res = res.collect()
    assert_frame_equal(res, exp)
