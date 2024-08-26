#![allow(clippy::unused_unit)]
use polars::prelude::*;
use pyo3_polars::derive::polars_expr;

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

#[polars_expr(output_type=String)]
fn remove_all_whitespace(inputs: &[Series]) -> PolarsResult<Series> {
    // removes all whitespace with a single space
    let ca: &StringChunked = inputs[0].str()?;
    let out: StringChunked = ca.apply_to_buffer(|value: &str, output: &mut String| {
        if value.chars().next().is_some() {
            let n = value.chars().count();
            let mut first_non_space_index = n;
            for (i, c) in value.chars().enumerate() {
                if c.is_whitespace() {
                    if first_non_space_index != n {
                        output.push_str(&value[first_non_space_index..i]);
                        first_non_space_index = n;
                    }
                } else if first_non_space_index == n {
                    first_non_space_index = i;
                }
            }
            output.push_str(&value[first_non_space_index..n]);
        }
    });
    Ok(out.into_series())
}

#[polars_expr(output_type=String)]
fn remove_non_word_characters(inputs: &[Series]) -> PolarsResult<Series> {
    // removes all non-word characters. "word characters" are [\w\s].
    let ca: &StringChunked = inputs[0].str()?;
    let out: StringChunked = ca.apply_to_buffer(|value: &str, output: &mut String| {
        if value.chars().next().is_some() {
            let n = value.chars().count();
            let mut first_word_index = n;
            for (i, c) in value.chars().enumerate() {
                if c.is_whitespace() || c.is_alphanumeric() || c == '_' {
                    if first_word_index == n {
                        first_word_index = i;
                    }
                } else if first_word_index != n {
                    output.push_str(&value[first_word_index..i]);
                    first_word_index = n;
                }
            }
            output.push_str(&value[first_word_index..n]);
        }
    });
    Ok(out.into_series())
}

#[polars_expr(output_type=String)]
fn anti_trim(inputs: &[Series]) -> PolarsResult<Series> {
    // removes all inner whitespace, but keeps leading and trailing whitespace
    let ca: &StringChunked = inputs[0].str()?;
    let out: StringChunked = ca.apply_to_buffer(|value: &str, output: &mut String| {
        if value.chars().next().is_some() {
            let n = value.chars().count();
            let mut is_first_space = true;
            let mut first_non_space_index = n;
            let mut most_recent_space_start = n;
            for (i, c) in value.chars().enumerate() {
                if c.is_whitespace() {
                    if first_non_space_index != n {
                        output.push_str(&value[first_non_space_index..i]);
                        first_non_space_index = n;
                    }
                    if most_recent_space_start == n {
                        most_recent_space_start = i;
                    }
                } else {
                    if is_first_space {
                        if i > 0 {
                            output.push_str(&value[0..i]);
                        }
                        is_first_space = false;
                    }
                    if first_non_space_index == n {
                        // value[most_recent_space_start..i] is all whitespace
                        first_non_space_index = i;
                        most_recent_space_start = n;
                    }
                }
            }
            output.push_str(&value[first_non_space_index..n]);
            output.push_str(&value[most_recent_space_start..n]);
        }
    });
    Ok(out.into_series())
}
