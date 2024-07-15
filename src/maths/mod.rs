#![allow(clippy::unused_unit)]
use polars::prelude::*;
use pyo3_polars::derive::polars_expr;
use pyo3_polars::export::polars_core::export::num::Signed;
use serde::Deserialize;

#[derive(Deserialize)]
pub struct OrElseKwargs {
    pub or_else: f64,
}
fn same_output_type(input_fields: &[Field]) -> PolarsResult<Field> {
    let field = &input_fields[0];
    Ok(field.clone())
}
fn impl_div_or_else<T>(
    divid: &ChunkedArray<T>,
    divis: &ChunkedArray<T>,
    or_else: T::Native,
    zero: T::Native,
) -> ChunkedArray<T>
where
    T: PolarsNumericType,
    T::Native: Signed,
{
    divid
        .into_iter()
        .zip(divis)
        .map(|(divid_opt, divis_opt)| {
            divid_opt.and_then(|dividend| {
                divis_opt.map(|divisor| {
                    if divisor == zero {
                        or_else
                    } else {
                        dividend / divisor
                    }
                })
            })
        })
        .collect()
}

#[polars_expr(output_type_func=same_output_type)]
fn div_or_else(inputs: &[Series], kwargs: OrElseKwargs) -> PolarsResult<Series> {
    let divid = &inputs[0];
    let divis = &inputs[1];
    let or_else = kwargs.or_else;
    match (divis.dtype(), divid.dtype()) {
        (DataType::Float64, _) | (_, DataType::Float64) => Ok(impl_div_or_else(
            divid.cast(&DataType::Float64)?.f64().unwrap(),
            divis.cast(&DataType::Float64)?.f64().unwrap(),
            or_else,
            0 as f64,
        )
        .into_series()),
        (DataType::Float32, _) | (_, DataType::Float32) => Ok(impl_div_or_else(
            divid.cast(&DataType::Float32)?.f32().unwrap(),
            divis.cast(&DataType::Float32)?.f32().unwrap(),
            or_else as f32,
            0 as f32,
        )
        .into_series()),
        (DataType::Int64, _) | (_, DataType::Int64) => Ok(impl_div_or_else(
            divid.cast(&DataType::Int64)?.i64().unwrap(),
            divis.cast(&DataType::Int64)?.i64().unwrap(),
            or_else.round() as i64,
            0_i64,
        )
        .into_series()),
        (DataType::Int32, _) | (_, DataType::Int32) => Ok(impl_div_or_else(
            divid.cast(&DataType::Int32)?.i32().unwrap(),
            divis.cast(&DataType::Int32)?.i32().unwrap(),
            or_else.round() as i32,
            0_i32,
        )
        .into_series()),
        _ => {
            polars_bail!(InvalidOperation:format!("dtypes not \
            supported for div_or_else, expected Int32, Int64, Float32, Float64."))
        }
    }
}
