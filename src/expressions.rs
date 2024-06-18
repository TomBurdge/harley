#![allow(clippy::unused_unit)]
use polars::prelude::*;
use pyo3_polars::derive::polars_expr;
use std::fmt::Write;

// #[polars_expr(output_type=String)]
// fn pig_latinnify(inputs: &[Series]) -> PolarsResult<Series> {
//     let ca: &StringChunked = inputs[0].str()?;
//     let out: StringChunked = ca.apply_to_buffer(|value: &str, output: &mut String| {
//         if let Some(first_char) = value.chars().next() {
//             write!(output, "{}{}ay", &value[1..], first_char).unwrap()
//         }
//     });
//     Ok(out.into_series())
// }

#[polars_expr(output_type=String)]
fn single_space(inputs: &[Series]) -> PolarsResult<Series> {
    let ca: &StringChunked = inputs[0].str()?;
    let out: StringChunked = ca.apply_to_buffer(|value: &str, output: &mut String| {
        if value == "" {return;}
        let mut start_with_white_space = value.chars().next().unwrap().is_whitespace();
        let mut to_add_space = false;
        for c in value.chars() {
            if c.is_whitespace() {
                to_add_space = true;
            } else {
                if to_add_space {
                    if !start_with_white_space {
                        write!(output, " ").unwrap();
                    }
                    start_with_white_space = false;
                    to_add_space = false;
                }
                write!(output, "{}", c).unwrap();
            }
        }
    });
    Ok(out.into_series())
}