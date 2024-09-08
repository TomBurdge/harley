# Harley

![harley logo](images/harley.jpg)

Harley contains polars helper methods that will make you more productive.

Checkout the full code documentation [here](https://tomburdge.github.io/harley/reference/harley).

Harley is also a great way to learn about polars best practices, like how to use plugins for custom functionality at speed.

Harley is a polars port of Harley's sister project, [Quinn](https://github.com/MrPowers/quinn). 
Many of Quinn's original methods, written to extend pyspark, are trivial with the excellent polars API.

Harley is neither associated with motorbikes, nor the DC universe.

## Getting started
In addition to reading the docs, you can pip install harley with:
```python
python -m pip install harley
```


## Contributing

Want to contribute to Harley?
You are more than welcome!

Anything that would improve your own polars development experience is a great candidate for a feature.

If you wish to write some rust via a polars plugin with your feature, [Marco Gorelli's](https://marcogorelli.github.io/polars-plugins-tutorial/) tutorial is a perfect place to start.

### Code Style

We use `sphinx` as docstrings format. For more details about `sphinx` format see [this tutorial](https://sphinx-rtd-tutorial.readthedocs.io/en/latest/docstrings.html). A short example of `sphinx`-formatted docstring is placed below:

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