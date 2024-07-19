use polars::prelude::*;
use pyo3_polars::derive::polars_expr;
use serde::Deserialize;
use rand::prelude::*;

#[derive(Deserialize)]
pub struct SeedKwargs {
    pub seed: Option<i64>,
}

fn output_type_from_list_type(input_fields: &[Field]) -> PolarsResult<Field> {
    if !input_fields[0].data_type().is_list() {
        polars_bail!(ComputeError: "Expected list type")
    } else {
        let name = input_fields[0].name();
        Ok(Field::new(name, input_fields[0].data_type().inner_dtype().unwrap().clone()))
    }
}

fn _array_choice<T>(rng: &mut StdRng, ca: ChunkedArray<DataType::List(Box<T>)>) -> ChunkedArray<T> where T: PolarsDataType {
    ca.into_iter().map(|elems| {
        elems.and_then(|list_elem| {
            let len = list_elem.len();
            let idx = rng.gen_range(0..len);
            Some(list_elem.get(idx))
        })
        }
    ).collect()
}

#[polars_expr(output_type_func=output_type_from_list_type)]
fn array_choice(inputs: &[Series], kwargs: SeedKwargs) -> PolarsResult<Series> {
    if !inputs[0].dtype().is_list() {
        polars_bail!(ComputeError: "Expected list type")
    }
    let seed = kwargs.seed;
    let mut rng;
    match seed {
        Some(x) => {
            if x <= 0 {
                polars_bail!(ComputeError: "seed must be greater than 0")
            } else {
                rng = StdRng::seed_from_u64(x as u64);
            }
        }
        None => {
            rng = StdRng::from_entropy();
        }
    }
    let ca = inputs[0].list().unwrap();
    let out = _array_choice(rng, ca);
    Ok(out.into_series())
}