from harley.schema_helpers import print_schema_as_code
from typing import OrderedDict
import polars.datatypes as T
import pytest
from io import StringIO
import sys


@pytest.mark.parametrize(
    "schema, prepend_pl, exp",
    [
        (
            OrderedDict([("col_1", T.String), ("col_2", T.Array)]),
            True,
            'OrderedDict([("col_1", pl.String),("col_2", pl.Array),])',
        ),
        (
            OrderedDict([("col_1", T.String), ("col_2", T.Array)]),
            False,
            'OrderedDict([("col_1", String),("col_2", Array),])',
        ),
        (
            OrderedDict([("col_1", T.String)]),
            False,
            'OrderedDict([("col_1", String),])',
        ),
        (
            OrderedDict([("col_1", T.String), ("col_2", T.Array), ("col_3", T.Int64)]),
            False,
            'OrderedDict([("col_1", String),("col_2", Array),("col_3", Int64),])',
        ),
    ],
)
def test_print_schema_as_code(schema: OrderedDict, prepend_pl: bool, exp: str):
    captured_output = StringIO()
    sys.stdout = captured_output
    print_schema_as_code(schema, prepend_pl)
    sys.stdout = sys.__stdout__
    res = captured_output.getvalue().rstrip("\n")
    assert res == exp
