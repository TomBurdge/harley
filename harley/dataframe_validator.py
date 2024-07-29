from typing import Dict, List, OrderedDict, Union

from polars import LazyFrame

from harley.utils import PolarsFrame


class DataFrameMissingColumnError(ValueError):
    """Raise this when there's a DataFrame column error."""


class DataSchemaError(ValueError):
    """Raise this when schema validation fails"""


class DataFrameProhibitedColumnError(ValueError):
    """Raise this when a DataFrame includes prohibited columns."""


def validate_presence_of_columns(
    df: PolarsFrame, required_col_names: list[str]
) -> None:
    """Validate the presence of column names in a DataFrame.

    :param df: A polars DataFrame or LazyFrame
    :param required_col_names: List of the required column names for the DataFrame.
    :raises DataFrameMissingColumnError: if any of the requested column names are
    not present in the DataFrame.
    """
    if isinstance(df, LazyFrame):
        all_col_names = df.collect_schema().names()
    else:
        all_col_names = df.columns
    missing_col_names = [x for x in required_col_names if x not in all_col_names]
    error_message = f"The {missing_col_names} columns are not included in the DataFrame with the following columns {all_col_names}"
    if missing_col_names:
        raise DataFrameMissingColumnError(error_message)


def validate_schema(df: PolarsFrame, required_schema: Union[Dict, OrderedDict]) -> None:
    """Validate the schema of a DataFrame.

    :param df: A polars DataFrame or LazyFrame
    :param required_schema: The expected schema for the DataFrame.
    :raises DataSchemaError: if the DataFrame does not match the expected schema.
    """
    if isinstance(df, LazyFrame):
        df_schema = df.collect_schema()
    else:
        df_schema = df.schema
    if isinstance(required_schema, dict):
        df_schema = dict(df_schema)
    if not required_schema == df_schema:
        raise DataSchemaError("The Frame did not match the expected schema")


def validate_absence_of_columns(
    df: PolarsFrame, prohibited_col_names: List[str]
) -> None:
    """Validate the absence of column names in a DataFrame.

    :param df: A polars DataFrame or LazyFrame
    :param prohibited_col_names: List of the prohibited column names for the DataFrame.
    :raises DataFrameProhibitedColumnError: if any of the requested column names are
    present in the DataFrame.
    """
    if isinstance(df, LazyFrame):
        df_cols = df.collect_schema().names()
    else:
        df_cols = df.columns
    present_prohibited_col_names = [
        col for col in prohibited_col_names if col in df_cols
    ]
    if present_prohibited_col_names:
        raise DataFrameProhibitedColumnError(
            "Prohibited columns present:", present_prohibited_col_names
        )
