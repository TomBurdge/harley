import pytest
from harley.transformations import snake_case_column_names, flatten_struct
from polars import DataFrame, LazyFrame, Series, DataType
from typing import Union
from tests.conftest import polars_frames
from polars.testing import assert_frame_equal
from typing import List

column_names = [
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


@pytest.mark.parametrize("frame_type", polars_frames)
@pytest.mark.parametrize(
    "input_column, exp",
    column_names,
)
def test_snake_case_column_names(
    frame_type: Union[LazyFrame, DataFrame], input_column: str, exp: str
):
    polars_frame = frame_type(data={input_column: range(10)})
    res = snake_case_column_names(polars_frame).columns
    assert res == [exp]


@pytest.fixture()
def data_with_struct() -> List[Series]:
    data = [
        Series(
            "ratings",
            [
                {"movie": "Cars", "theatre": "NE", "avg_rating": 4.5},
                {"movie": "Toy Story", "theatre": "ME", "avg_rating": 4.9},
            ],
        ),
        Series("name", ["George", "Yi Fong"]),
    ]
    return data


@pytest.mark.parametrize("frame_type", polars_frames)
@pytest.mark.timeout(1)
def test_flatten_struct(frame_type: DataType, data_with_struct: List[Series]):
    inp = frame_type(data_with_struct)
    exp = DataFrame(
        [
            Series("name", ["George", "Yi Fong"]),
            Series("ratings:movie", ["Cars", "Toy Story"]),
            Series("ratings:theatre", ["NE", "ME"]),
            Series("ratings:avg_rating", [4.5, 4.9]),
        ]
    )
    if isinstance(res := flatten_struct(inp, "ratings"), LazyFrame):
        res = res.collect()
    assert_frame_equal(res, exp)


@pytest.mark.parametrize("frame_type", polars_frames)
@pytest.mark.timeout(1)
def test_flatten_struct_do_not_drop(
    frame_type: DataType, data_with_struct: List[Series]
):
    inp = frame_type(data_with_struct)
    exp = DataFrame(
        [
            Series("name", ["George", "Yi Fong"]),
            Series("ratings:movie", ["Cars", "Toy Story"]),
            Series("ratings:theatre", ["NE", "ME"]),
            Series("ratings:avg_rating", [4.5, 4.9]),
            Series(
                "ratings",
                [
                    {"movie": "Cars", "theatre": "NE", "avg_rating": 4.5},
                    {"movie": "Toy Story", "theatre": "ME", "avg_rating": 4.9},
                ],
            ),
        ]
    )
    if isinstance(
        res := flatten_struct(inp, "ratings", drop_original_struct=False), LazyFrame
    ):
        res = res.collect()
    assert_frame_equal(res, exp, check_column_order=False)


@pytest.mark.parametrize("frame_type", polars_frames)
@pytest.mark.timeout(1)
def test_flatten_struct_separator(frame_type: DataType, data_with_struct: List[Series]):
    inp = frame_type(data_with_struct)
    exp = DataFrame(
        [
            Series("name", ["George", "Yi Fong"]),
            Series("ratings_movie", ["Cars", "Toy Story"]),
            Series("ratings_theatre", ["NE", "ME"]),
            Series("ratings_avg_rating", [4.5, 4.9]),
        ]
    )
    if isinstance(res := flatten_struct(inp, "ratings", separator="_"), LazyFrame):
        res = res.collect()
    assert_frame_equal(res, exp)


@pytest.fixture()
def nested_struct_data() -> dict:
    data = {
        "coords": [
            {"x": {"z": 1, "a": {"b": 2}}, "y": 4},
            {"x": {"z": 3, "a": {"b": 4}}, "y": 9},
            {"x": {"z": 5, "a": {"b": 6}}, "y": 16},
        ],
        "multiply": [10, 2, 3],
    }
    return data


@pytest.mark.parametrize("frame_type", polars_frames)
@pytest.mark.timeout(1)
def test_flatten_struct_recursive(
    frame_type: DataType, nested_struct_data: List[Series]
):
    inp = frame_type(nested_struct_data)
    exp = DataFrame(
        [
            Series("coords:x:z", [1, 3, 5]),
            Series("coords:x:a:b", [2, 4, 6]),
            Series("coords:y", [4, 9, 16]),
            Series("multiply", [10, 2, 3]),
        ]
    )
    if isinstance(
        res := flatten_struct(df=inp, struct_columns="coords", recursive=True),
        LazyFrame,
    ):
        res = res.collect()
    assert_frame_equal(res, exp, check_column_order=False)


@pytest.mark.parametrize("frame_type", polars_frames)
@pytest.mark.timeout(1)
def test_flatten_struct_recursive_limit(
    frame_type: DataType, nested_struct_data: List[Series]
):
    inp = frame_type(nested_struct_data)
    exp = DataFrame(
        [
            Series("coords:x:z", [1, 3, 5]),
            Series("coords:x:a", [{"b": 2}, {"b": 4}, {"b": 6}]),
            Series("coords:y", [4, 9, 16]),
            Series("multiply", [10, 2, 3]),
        ]
    )
    if isinstance(
        res := flatten_struct(df=inp, struct_columns="coords", recursive=True, limit=2),
        LazyFrame,
    ):
        res = res.collect()
    assert_frame_equal(res, exp, check_column_order=False)
