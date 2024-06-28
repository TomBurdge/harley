mod utils;
mod is_null;
mod spaces;
mod column_rename;
use column_rename::columns_to_snake_case;

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
    m.add_function(wrap_pyfunction!(columns_to_snake_case, m)?)?;
    Ok(())
}
