# Harley

Harley is a library of helper functions for dealing with polars dataframes, inspired by [quinn](https://github.com/MrPowers/quinn).

## Contributing


### Code Style

We are using [PySpark code-style](https://github.com/MrPowers/spark-style-guide/blob/main/PYSPARK_STYLE_GUIDE.md) and `sphinx` as docstrings format. For more details about `sphinx` format see [this tutorial](https://sphinx-rtd-tutorial.readthedocs.io/en/latest/docstrings.html). A short example of `sphinx`-formatted docstring is placed below:

```python
"""[Summary]

:param [ParamName]: [ParamDescription], defaults to [DefaultParamVal]
:type [ParamName]: [ParamType](, optional)
...
:raises [ErrorType]: [ErrorDescription]
...
:return: [ReturnDescription]
:rtype: [ReturnType]
"""
```