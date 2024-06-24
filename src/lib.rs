mod expressions;
mod utils;
use expressions::two_columns_to_dictionary;

#[cfg(target_os = "linux")]
use jemallocator::Jemalloc;

#[global_allocator]
#[cfg(target_os = "linux")]
static ALLOC: Jemalloc = Jemalloc;

use pyo3::types::PyModule;
use pyo3::Python;
use pyo3::{pymodule, Bound, PyResult, wrap_pyfunction};

#[pymodule]
fn harley(_py: Python<'_>, m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add("__version__", env!("CARGO_PKG_VERSION"))?;
    m.add_function(wrap_pyfunction!(two_columns_to_dictionary, m)?)?;
    Ok(())
}
