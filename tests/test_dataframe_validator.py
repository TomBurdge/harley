import polars as pl
from harley.dataframe_validator import DataFrameMissingColumnError, validate_presence_of_columns
import pytest

def test_validate_presence_of_columns_fail():
    data = [("jose", 1), ("li", 2), ("luisa", 3)]
    source_df = pl.DataFrame(data, ["name", "age"])
    with pytest.raises(DataFrameMissingColumnError) as excinfo:
        validate_presence_of_columns(source_df, ["name", "age", "fun"])
    assert (
        excinfo.value.args[0]
        == "The ['fun'] columns are not included in the DataFrame with the following columns ['name', 'age']"
    )

def test_validate_presence_of_columns_pass():
    data = [("jose", 1), ("li", 2), ("luisa", 3)]
    source_df = pl.DataFrame(data, ["name", "age"])
    validate_presence_of_columns(source_df, ["name"])