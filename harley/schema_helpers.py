from typing import OrderedDict
from polars import DataType


def print_schema_as_code(
    schema: OrderedDict[str, DataType], prepend_pl: bool = False
) -> None:
    """
    Takes a schema as input and prints it as valid code, with an
    option to prepend 'pl.' to the column types.

    :param schema: An OrderedDict where the keys are column names (strings)
    and the values are data types (DataType)
    :type schema: OrderedDict[str, DataType]
    :param prepend_pl: The `prepend_pl` parameter is a boolean flag that determines whether to prepend
    'pl.' to the column types in the schema before printing it as code. If `prepend_pl` is set to True,
    each column type in the schema will be prefixed with 'pl.', defaults to False
    :type prepend_pl: bool (optional)
    """
    if prepend_pl:
        schema = OrderedDict(
            [(field, "pl." + str(d_type)) for field, d_type in schema.items()]
        )
    out = "OrderedDict(["
    for name, d_type in schema.items():
        out += '("' + name + '", ' + str(d_type) + "),"
    out += "])"
    print(out)
