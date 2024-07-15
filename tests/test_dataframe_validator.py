import polars as pl
from harley.dataframe_validator import (
    DataFrameMissingColumnError,
    validate_presence_of_columns,
    DataSchemaError,
    validate_schema,
    validate_absence_of_columns,
    DataFrameProhibitedColumnError,
)
import pytest
from typing import OrderedDict, Union, Dict
from tests.conftest import polars_frames

age_name_data = {"age": [1, 2, 3], "name": ["jose", "li", "luisa"]}


@pytest.mark.parametrize("frame_type", polars_frames)
def test_validate_presence_of_columns_fail(frame_type: pl.DataType):
    source_df = frame_type(age_name_data)
    with pytest.raises(DataFrameMissingColumnError) as exec_info:
        validate_presence_of_columns(source_df, ["name", "age", "fun"])
    assert (
        exec_info.value.args[0]
        == "The ['fun'] columns are not included in the DataFrame with the following columns ['age', 'name']"
    )


@pytest.mark.parametrize("frame_type", polars_frames)
def test_validate_presence_of_columns_pass(frame_type: pl.DataType):
    source_df = frame_type(age_name_data)
    validate_presence_of_columns(source_df, ["name"])


def create_dict_parameters(frames=polars_frames, dict_types=[dict, OrderedDict]):
    parameter_params = []
    for frame in frames:
        for dict_type in dict_types:
            parameter_params.append((frame, dict_type))
    return parameter_params


pytest_dict_frame_parameters = create_dict_parameters()


@pytest.mark.parametrize("frame_type, dict_type", pytest_dict_frame_parameters)
def test_validate_schema_raises_when_struct_field_missing(
    frame_type: pl.DataType, dict_type: Union[Dict, OrderedDict]
):
    source_df = frame_type(age_name_data)
    required_schema = dict_type(
        [
            ("name", pl.Utf8),
            ("city", pl.Utf8),
        ]
    )
    with pytest.raises(DataSchemaError):
        validate_schema(source_df, required_schema)


@pytest.mark.parametrize("frame_type, dict_type", pytest_dict_frame_parameters)
def test_validate_schema_does_nothing_when_schema_matches(
    frame_type: pl.DataType, dict_type: Union[Dict, OrderedDict]
):
    df = frame_type(age_name_data)
    required_schema = dict_type(
        [
            ("name", pl.Utf8),
            ("age", pl.Int64),
        ]
    )
    validate_schema(df, required_schema)


@pytest.mark.parametrize("frame_type", polars_frames)
def test_validate_absence_of_columns_passes_when_no_match(frame_type: pl.DataType):
    df = frame_type(age_name_data)
    prohibited_columns = ["fun", "ip_address"]
    validate_absence_of_columns(df=df, prohibited_col_names=prohibited_columns)


@pytest.mark.parametrize("frame_type", polars_frames)
def test_validate_absence_of_columns_fails_when_match(frame_type: pl.DataType):
    df = frame_type(age_name_data)
    prohibited_columns = ["name"]
    with pytest.raises(DataFrameProhibitedColumnError):
        validate_absence_of_columns(df=df, prohibited_col_names=prohibited_columns)
