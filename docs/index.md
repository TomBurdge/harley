# Harley

![harley logo](images/harley.jpg)

Harley contains polars helper methods that will make you more productive.

Harley is also a great way to learn about polars best practices like how to organize and unit test your code.

Harley is a port to polars of Harley's sister project, [quinn](https://github.com/MrPowers/quinn).

Harley is neither associated with motorbikes, nor the DC universe.

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