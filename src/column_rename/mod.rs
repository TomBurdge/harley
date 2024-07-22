#![allow(clippy::unused_unit)]
use heck::ToSnakeCase;
use pyo3::prelude::*;
use pyo3::types::PyDict;

#[pyfunction]
pub fn columns_to_snake_case(py: Python, columns: Vec<String>) -> PyResult<Py<PyDict>> {
    let py_dict = PyDict::new_bound(py);
    for col in columns.iter() {
        py_dict.set_item(col, col.to_snake_case())?;
    }
    Ok(py_dict.into())
}
