from harley.schema_helpers import print_schema_as_code
from typing import OrderedDict
import polars.datatypes as T
import pytest
from io import StringIO
import sys

@pytest.mark.parametrize("prepend_pl, exp", [(True, 'OrderedDict([("col_1", pl.String),("col_2", pl.Array),])'),(False,'OrderedDict([("col_1", String),("col_2", Array),])')])
def test_print_schema_as_code(prepend_pl:bool, exp:str):
    captured_output = StringIO()
    sys.stdout = captured_output
    schema = OrderedDict([("col_1", T.String), ("col_2", T.Array)])
    print_schema_as_code(schema, prepend_pl)
    sys.stdout = sys.__stdout__
    res = captured_output.getvalue().rstrip("\n")
    assert res ==exp