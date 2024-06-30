from typing import OrderedDict
from polars import DataType

def print_schema_as_code(schema: OrderedDict[str, DataType], prepend_pl: bool = False)-> str:
    if prepend_pl:
        schema = OrderedDict(
        [
            (field, "pl."+ str(d_type))
            for field, d_type in schema.items()
        ]
    )
    out = "OrderedDict(["
    for name, d_type in schema.items():
        out += '("' + name + '", ' + str(d_type) + '),'
    out += "])"
    print(out)
