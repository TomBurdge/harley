import polars as pl
from harley.dataframe_validator import DataFrameMissingColumnError, validate_presence_of_columns, DataSchemaError, validate_schema
import pytest
from polars import DataFrame, LazyFrame
from typing import OrderedDict, Union, Dict

polars_frames = [DataFrame, LazyFrame]

@pytest.mark.parametrize("frame_type",polars_frames)
def test_validate_presence_of_columns_fail(frame_type: pl.DataType):
    data = [("jose", 1), ("li", 2), ("luisa", 3)]
    source_df = frame_type(data, ["name", "age"])
    with pytest.raises(DataFrameMissingColumnError) as excinfo:
        validate_presence_of_columns(source_df, ["name", "age", "fun"])
    assert (
        excinfo.value.args[0]
        == "The ['fun'] columns are not included in the DataFrame with the following columns ['name', 'age']"
    )

@pytest.mark.parametrize("frame_type",polars_frames)
def test_validate_presence_of_columns_pass(frame_type: pl.DataType):
    data = [("jose", 1), ("li", 2), ("luisa", 3)]
    source_df = frame_type(data, ["name", "age"])
    validate_presence_of_columns(source_df, ["name"])


def create_dict_parameters(frames = polars_frames, dict_types= [dict, OrderedDict]):
    parameter_params = []
    for frame in frames:
        for dict_type in dict_types:
            parameter_params.append((frame, dict_type))
    return parameter_params

pytest_dict_frame_parameters = create_dict_parameters()

@pytest.mark.parametrize("frame_type, dict_type",pytest_dict_frame_parameters)
def test_validate_schema_raises_when_struct_field_missing(frame_type: pl.DataType, dict_type:Union[Dict, OrderedDict]):
    data = [("jose", 1), ("li", 2), ("luisa", 3)]
    source_df = frame_type(data, ["name", "age"])
    required_schema = dict_type([
        ("name", pl.Utf8),
        ("city", pl.Utf8),
    ])
    with pytest.raises(DataSchemaError):
        validate_schema(source_df, required_schema)

@pytest.mark.parametrize("frame_type, dict_type",pytest_dict_frame_parameters)
def test_validate_schema_does_nothing_when_schema_matches(frame_type: pl.DataType, dict_type:Union[Dict, OrderedDict]):
    data = [("jose", 1), ("li", 2), ("luisa", 3)]
    source_df = frame_type(data, ["name", "age"])
    required_schema = dict_type([
        ("name", pl.Utf8),
        ("age", pl.Int64),
    ])
    print(required_schema)
    print(source_df.schema)
    validate_schema(source_df, required_schema)
