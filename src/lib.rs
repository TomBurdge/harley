mod column_rename;
mod is_booley;
mod is_null;
mod maths;
mod spaces;
mod utils;
use column_rename::columns_to_snake_case;

#[cfg(target_os = "linux")]
use jemallocator::Jemalloc;

#[global_allocator]
#[cfg(target_os = "linux")]
static ALLOC: Jemalloc = Jemalloc;

use pyo3::types::PyModule;
use pyo3::Python;
use pyo3::{pymodule, wrap_pyfunction, Bound, PyResult};

#[pymodule]
fn harley(_py: Python<'_>, m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add("__version__", env!("CARGO_PKG_VERSION"))?;
    m.add_function(wrap_pyfunction!(columns_to_snake_case, m)?)?;
    Ok(())
}
