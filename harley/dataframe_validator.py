from harley.utils import PolarsFrame
from polars import LazyFrame
from typing import Dict, OrderedDict, Union, List


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

    :param df: A spark DataFrame.
    :type df: DataFrame`
    :param required_col_names: List of the required column names for the DataFrame.
    :type required_col_names: :py:class:`list` of :py:class:`str`
    :return: None.
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
