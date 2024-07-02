#![allow(clippy::unused_unit)]
use polars::prelude::*;
use pyo3_polars::derive::polars_expr;
use pyo3_polars::export::polars_core::export::num::Signed;
use serde::Deserialize;

#[derive(Deserialize)]
pub struct ThresholdKwargs {
    pub threshold: f64,
}

fn impl_approx_equal_numeric<T>(ca: &ChunkedArray<T>, threshold: T::Native) -> BooleanChunked
where
    T: PolarsNumericType,
    T::Native: Signed,
{
    ca
    .into_iter()
    .map(|opt_v| opt_v.map(|v| v.abs() <= threshold))
    .collect()
}

#[polars_expr(output_type=Boolean)]
fn approx_equal(inputs: &[Series], kwargs: ThresholdKwargs) -> PolarsResult<Series> {
    let s = &inputs[0] - &inputs[1];
    let threshold = kwargs.threshold;
    // need to handle the precision better... just cast to float when receive?
    match s.dtype() {
        DataType::Int32 => Ok(impl_approx_equal_numeric(s.i32().unwrap(), threshold.floor() as i32).into_series()),
        DataType::Int64 => Ok(impl_approx_equal_numeric(s.i64().unwrap(), threshold.floor() as i64).into_series()),
        DataType::Float32 => Ok(impl_approx_equal_numeric(s.f32().unwrap(), threshold as f32).into_series()),
        DataType::Float64 => Ok(impl_approx_equal_numeric(s.f64().unwrap(), threshold).into_series()),
        dtype => {
            polars_bail!(InvalidOperation:format!("dtype {dtype} not \
            supported for abs_numeric, expected Int32, Int64, Float32, Float64."))
        }
    }
}
