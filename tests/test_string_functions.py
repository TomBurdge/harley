import pytest

import polars as pl
from polars.testing import assert_series_equal

import harley

@pytest.mark.parametrize("lazy", [True, False])
def test_single_space(lazy: bool):
    string_data = [
        ("  I  go   ", "I go"),
        ("    to", "to"),
        ("school   ", "school"),
        ("by bus", "by bus"),
        (None, None),
    ]
    frame = pl.DataFrame([{"input": inp, "expected": exp} for inp, exp in string_data])
    if lazy:
        frame = frame.lazy()
    frame = frame.with_columns(
        actual = harley.single_space("input")
    )
    if lazy:
        frame = frame.collect()
    assert_series_equal(frame["actual"], frame["expected"], check_names=False)

@pytest.mark.parametrize("lazy", [True, False])
def test_single_space_by_chars(lazy: bool):
    string_data = [
        ("  I\t\r\v\n go   ", "I go"),
        ("    to", "to"),
        ("school   ", "school"),
        ("by bus", "by bus"),
        (None, None),
    ]
    frame = pl.DataFrame([{"input": inp, "expected": exp} for inp, exp in string_data])
    if lazy:
        frame = frame.lazy()
    frame = frame.with_columns(
        actual = harley.single_space("input")
    )
    if lazy:
        frame = frame.collect()
    assert_series_equal(frame["actual"], frame["expected"], check_names=False)