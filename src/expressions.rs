#![allow(clippy::unused_unit)]
use pyo3::prelude::*;
use pyo3::types::PyDict;
use heck::ToSnakeCase;

#[pyfunction]
pub fn columns_to_snake_case(py:Python, columns: Vec<String>) -> PyResult<Py<PyDict>> {
    let py_dict = PyDict::new_bound(py);
    for col in columns.iter(){
        py_dict.set_item(col, col.to_snake_case())?;
    }
    Ok(py_dict.into())
}

#[polars_expr(output_type=String)]
fn single_space(inputs: &[Series]) -> PolarsResult<Series> {
    // replaces all whitespace with a single space, then removes leading and trailing spaces

    let ca: &StringChunked = inputs[0].str()?;
    let out: StringChunked = ca.apply_to_buffer(|value: &str, output: &mut String| {
        if value.chars().next().is_some() {
            let n = value.chars().count();
            let mut first_non_space_index = n;
            let mut has_space = false;
            let mut first_space = true;
            for (i, c) in value.chars().enumerate() {
                if c.is_whitespace() {
                    has_space = true;
                    if first_non_space_index != n {
                        output.push_str(&value[first_non_space_index..i]);
                        first_non_space_index = n;
                    }
                } else {
                    if has_space {
                        if !first_space {
                            output.push(' ');
                        }
                        has_space = false;
                    }
                    first_space = false;
                    if first_non_space_index == n {
                        first_non_space_index = i;
                    }
                }
            }
            output.push_str(&value[first_non_space_index..n]);
        }
    });
    Ok(out.into_series())
}