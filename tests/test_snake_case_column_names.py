import pytest
from harley.transformations import snake_case_column_names
from polars import DataFrame, LazyFrame
from typing import Union
from tests.conftest import combine_pytest_params_with_dtypes, polars_frames

age_name_data = {"age": [1, 2, 3], "name": ["jose", "li", "luisa"]}

columns = [
    ["CamelCase", "camel_case"],
    ["This is Human case.", "this_is_human_case"],
    ["   This is preceding white space case.", "this_is_preceding_white_space_case"],
    ["This is succeeding white space case   ", "this_is_succeeding_white_space_case"],
    ["this.would.confuse.a.sql.query", "this_would_confuse_a_sql_query"],
    ["MixedUP CamelCase, with some Spaces", "mixed_up_camel_case_with_some_spaces"],
    [
        "mixed_up_ snake_case with some _spaces",
        "mixed_up_snake_case_with_some_spaces",
    ],
    ["kebab-case", "kebab_case"],
    ["SHOUTY_SNAKE_CASE", "shouty_snake_case"],
    ["snake_case", "snake_case"],
    ["ABcDE", "a_bc_de"],
    ["abcDEF", "abc_def"],
    ["ABC123dEEf456FOO", "abc123d_e_ef456_foo"],
    [
        "this-contains_ ALLKinds OfWord_Boundaries",
        "this_contains_all_kinds_of_word_boundaries",
    ],
    ["XΣXΣ baﬄe", "xσxς_baﬄe"],
    ["XMLHttpRequest", "xml_http_request"],
    ["FIELD_NAME11", "field_name11"],
    ["99BOTTLES", "99bottles"],
    ["FieldNamE11", "field_nam_e11"],
    ["abc123def456", "abc123def456"],
    ["abc123DEF456", "abc123_def456"],
    ["abc123Def456", "abc123_def456"],
    ["abc123DEf456", "abc123_d_ef456"],
    ["ABC123def456", "abc123def456"],
    ["ABC123DEF456", "abc123def456"],
    ["ABC123Def456", "abc123_def456"],
    ["ABC123DEf456", "abc123d_ef456"],
]
frame_column_combinations = combine_pytest_params_with_dtypes(polars_frames, columns)

@pytest.mark.parametrize(
    "frame_type, input_column, exp",
    frame_column_combinations,
)
def test_snake_case_column_names(
    frame_type: Union[LazyFrame, DataFrame], input_column: str, exp: str
):
    polars_frame = frame_type(data={input_column: range(10)})
    res = snake_case_column_names(polars_frame).columns
    assert res == [exp]
