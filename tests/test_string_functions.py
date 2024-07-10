import pytest

import polars as pl
from polars.testing import assert_series_equal
from string import ascii_letters

import harley


@pytest.mark.parametrize("lazy", [True, False])
def test_single_space(lazy: bool):
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
    frame = frame.with_columns(actual=harley.single_space("input"))
    if lazy:
        frame = frame.collect()
    assert_series_equal(frame["actual"], frame["expected"], check_names=False)

@pytest.mark.parametrize("lazy", [True, False])
def test_remove_all_whitespace(lazy: bool):
    string_data = [
        ("  I\t\r\v\n go   ", "Igo"),
        ("    to", "to"),
        ("school   ", "school"),
        ("by bus", "bybus"),
        (None, None),
    ]
    frame = pl.DataFrame([{"input": inp, "expected": exp} for inp, exp in string_data])
    if lazy:
        frame = frame.lazy()
    frame = frame.with_columns(actual=harley.remove_all_whitespace("input"))
    if lazy:
        frame = frame.collect()
    assert_series_equal(frame["actual"], frame["expected"], check_names=False)

@pytest.mark.parametrize("lazy", [True, False])
def test_remove_non_space_characters(lazy: bool):
    string_data = [
        ("\t\r\v\n ", "\t\r\v\n "),
        (ascii_letters, ascii_letters),
        ("1234567890", "1234567890"),
        ("_", "_"),
        ("", ""),
        ("!@#$%^&*()", ""),
        ("!I @go #to $school_%by *bus.?", "I go to school_by bus"),
        (None, None),
    ]
    frame = pl.DataFrame([{"input": inp, "expected": exp} for inp, exp in string_data])
    if lazy:
        frame = frame.lazy()
    frame = frame.with_columns(actual=harley.remove_non_word_characters("input"))
    if lazy:
        frame = frame.collect()
    assert_series_equal(frame["actual"], frame["expected"], check_names=False)

@pytest.mark.parametrize("lazy", [True, False])
def test_anti_trim(lazy: bool):
    string_data = [
        ("  I\t\r\v\n go   ", "  Igo   "),
        ("\t\r\v\n to", "\t\r\v\n to"),
        ("school   ", "school   "),
        ("by bus", "bybus"),
        ("    ", "    "),
        ("", ""),
        (None, None),
    ]
    frame = pl.DataFrame([{"input": inp, "expected": exp} for inp, exp in string_data])
    if lazy:
        frame = frame.lazy()
    frame = frame.with_columns(actual=harley.anti_trim("input"))
    if lazy:
        frame = frame.collect()
    assert_series_equal(frame["actual"], frame["expected"], check_names=False)
