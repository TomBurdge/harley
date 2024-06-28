#![allow(clippy::unused_unit)]
use polars::prelude::*;
use pyo3_polars::derive::polars_expr;
use serde::Deserialize;


#[derive(Debug)]
pub enum NullOrBlankSeries <'a> {
    StringSeries  {series: & 'a [Series], treat_all_white_space_as_blank: bool}
    , ListSeries {series: & 'a [Series]}
}

#[derive(Deserialize)]
pub struct NullOrBlankKwargs {
    pub all_white_space_as_null: bool,
}
pub trait FromKwargs{
    fn from_kwargs(data_type:&[Series] , all_white_space_as_null: bool) -> Result<NullOrBlankSeries, PolarsError>;
}

impl FromKwargs for NullOrBlankSeries<'_> {
    fn from_kwargs(series: &[Series], all_white_space_as_null: bool) -> Result<NullOrBlankSeries, PolarsError> {
        match series[0].dtype() {
            // TODO: ListType in the constructor OR a disjunction
            &DataType::List(_) => Ok(NullOrBlankSeries::ListSeries{series})
            , &DataType::String => Ok(NullOrBlankSeries::StringSeries {series, treat_all_white_space_as_blank: all_white_space_as_null })
            ,_ => Err(PolarsError::ComputeError(
                format!(
                    "Unsupported data type."
                )
                .into(),
            ))
        }

    }
}

trait ProcessIsNullOrBlanks {
    fn process_null_or_blanks(self, results: BooleanChunkedBuilder) -> Result<BooleanChunkedBuilder, PolarsError>;
}


impl ProcessIsNullOrBlanks for crate::is_null::NullOrBlankSeries <'_> {
    fn process_null_or_blanks(self, mut results: BooleanChunkedBuilder) -> Result<BooleanChunkedBuilder, PolarsError> {
        match self {
            // TODO: move this to use the abs numeric structure and use PolarsListType to deal with array & List
            // put the dtype in the enum struct, to do the impl function
            NullOrBlankSeries::ListSeries { series } => {
                let ca = series[0].list()?;
                ca.into_iter().for_each(|op_s| {
                    if let Some(s) = op_s{
                        match s.is_empty() {
                            true => results.append_value(true),
                            _ => results.append_value(false)
                        }
                    } else{
                        results.append_value(true)
                    }
                }
                );
                Ok(results)
            }
            , NullOrBlankSeries::StringSeries { series ,treat_all_white_space_as_blank} => {
                let ca = series[0].str()?;
                if treat_all_white_space_as_blank {
                ca.into_iter().for_each(|op_s| {
                    if let Some(s) = op_s{
                        if s.is_empty() { results.append_value(true)}
                        else {results.append_value(s.trim().is_empty())};
                    }
                    else {
                        results.append_value(true)
                    }
                }
                );
                }
                else {
                    ca.into_iter().for_each(|op_s| {
                        if let Some(s) = op_s{
                                results.append_value(s.is_empty());
                        } else{
                            results.append_value(true)
                        }
                    }
                    );
                }
                Ok(results)
            }
        }
    }
}

#[polars_expr(output_type=String)]
fn is_null_or_blank(inputs: &[Series], kwargs:NullOrBlankKwargs) -> PolarsResult<Series> {
    let mut results = BooleanChunkedBuilder::new("null_or_blank", inputs[0].len());
    let series_parser = NullOrBlankSeries::from_kwargs(inputs, kwargs.all_white_space_as_null)?;
    results = series_parser.process_null_or_blanks(results)?;
    let out = results.finish();
    Ok(out.into_series())
}
