#![allow(clippy::unused_unit)]
use polars::prelude::*;
use pyo3_polars::derive::polars_expr;

#[polars_expr(output_type=Boolean)]
fn is_falsey(inputs: &[Series]) -> PolarsResult<Series> {
    let mut results = BooleanChunkedBuilder::new("null_or_blank", inputs[0].len());
    let ca = inputs[0].bool()?;
    ca.into_iter().for_each(|op_s| {
        if let Some(s) = op_s{
            match s {
                false => results.append_value(true),
                _ => results.append_value(false)
            }
        } else{
            results.append_value(true)
        }
    }
    );
    let out = results.finish();
    Ok(out.into_series())
}

#[polars_expr(output_type=Boolean)]
fn is_false(inputs: &[Series]) -> PolarsResult<Series> {
    let mut results = BooleanChunkedBuilder::new("null_or_blank", inputs[0].len());
    let ca = inputs[0].bool()?;
    ca.into_iter().for_each(|op_s| {
        if let Some(s) = op_s{
            match s {
                false => results.append_value(true),
                _ => results.append_value(false)
            }
        } else{
            results.append_value(false)
        }
    }
    );
    let out = results.finish();
    Ok(out.into_series())
}

// surely this is just the same for is true/is truthy
// there are only three possible values for a boolean column: F/T/None
fn _is_true(inputs: &[Series]) -> PolarsResult<Series> {
    let mut results = BooleanChunkedBuilder::new("null_or_blank", inputs[0].len());
    let ca = inputs[0].bool()?;
    ca.into_iter().for_each(|op_s| {
        if let Some(s) = op_s{
            match s {
                true => results.append_value(true),
                _ => results.append_value(false)
            }
        } else{
            results.append_value(false)
        }
    }
    );
    let out = results.finish();
    Ok(out.into_series())
}

#[polars_expr(output_type=Boolean)]
fn is_truthy(inputs: &[Series]) -> PolarsResult<Series> {
    _is_true(inputs)
}

#[polars_expr(output_type=Boolean)]
fn is_true(inputs: &[Series]) -> PolarsResult<Series> {
    _is_true(inputs)
}