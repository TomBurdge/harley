#![allow(clippy::unused_unit)]
use polars::prelude::*;
use pyo3_polars::PyDataFrame;
use pyo3::prelude::*;
use pyo3::types::{PyDict, PyString,PyFloat};
use pyo3::exceptions::PyValueError;

#[pyfunction]
pub fn two_columns_to_dictionary(py: Python, pydf: PyDataFrame, col1: &str, col2: &str) -> PyResult<Py<PyDict>> {
    let df: DataFrame = pydf.into();
    let series1 = df.column(col1).map_err(|e| PyErr::new::<pyo3::exceptions::PyValueError, _>(format!("{}", e)))?;
    let series2 = df.column(col2).map_err(|e| PyErr::new::<pyo3::exceptions::PyValueError, _>(format!("{}", e)))?;

    fn convert_series_value(py: Python, value: AnyValue) -> PyResult<PyObject> {
        match value {
            AnyValue::String(val) => Ok(PyString::new_bound(py, val).into_py(py)),
            AnyValue::Int64(val) => Ok(val.into_py(py)),
            AnyValue::UInt64(val) => Ok((val as i64).into_py(py)), // Python's int can handle large integers
            AnyValue::Float64(val) => Ok(PyFloat::new_bound(py, val).into_py(py)),
            AnyValue::Null => Ok(py.None()), // Handle null values
            AnyValue::Boolean(val) => Ok(val.into_py(py)),
            AnyValue::Float32(val) => Ok(PyFloat::new_bound(py, val as f64).into_py(py)), // Convert Float32 to Float64
            AnyValue::Int32(val) => Ok(val.into_py(py)),
            AnyValue::UInt32(val) => Ok((val as i64).into_py(py)), // Python's int can handle large integers
            AnyValue::Int16(val) => Ok(val.into_py(py)),
            AnyValue::UInt16(val) => Ok((val as i64).into_py(py)), // Python's int can handle large integers
            AnyValue::Int8(val) => Ok(val.into_py(py)),
            AnyValue::UInt8(val) => Ok((val as i64).into_py(py)), // Python's int can handle large integers
            AnyValue::Date(val) => Ok(val.into_py(py)),
            AnyValue::Datetime(val, _, _) => Ok(val.into_py(py)),
            AnyValue::Duration(val, _) => Ok(val.into_py(py)),
            AnyValue::Time(val) => Ok(val.into_py(py)),
            _ => Err(PyErr::new::<PyValueError, _>("Unsupported data type")),
        }
    }

    let py_dict = PyDict::new_bound(py);
    let iter1 = series1.iter();
    let iter2 = series2.iter();

    for (val1, val2) in iter1.zip(iter2) {
        let key = convert_series_value(py, val1)?;
        let value = convert_series_value(py, val2)?;
        py_dict.set_item(key, value)?;
    }

    Ok(py_dict.into())
}
