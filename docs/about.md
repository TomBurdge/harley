# About Harley

## Overview

## Why

## Comparison with Quinn

While this project is heavily inspired by Quinn, we have chosen not to copy all its functionality (and implementation) one-to-one. Below we list:

- functions which we did not implement
- functions which we modified its scope
- functions which were implemented relatively faithfully

### Unimplemented Functions



1. Polars (or another common extension) covers it, e.g. column renaming.
2. Polars doesn't support it, or heavily discourages it, e.g. `map` type, 

- Dataframe helpers
    - `append_if_schema_identical`: polars append is strict on schema by default (separate namespace for diagonal append).
    - `show_output_to_df`: polars already has [init_repr_to_df](https://docs.pola.rs/api/python/stable/reference/api/polars.from_repr.html).
    - `create_df`: Already in the polars library.
- Functions
    - `exists`: We don't want to allow for an arbitrary python function as the callable. This requires inefficient map which will tank performance.
    - `forall`: See `exists`.
    - `is_not_in`: Covered in native polars.

### Changed Functions

### Remaining Functions

- dataframe_helpers
* [x] `column_to_list` - python. (this is just polars `select`, `collect`, `to_list`, much more convenient than spark).
* [x] `two_columns_to_dictionary` - python
* [x] `to_list_of_dictionaries` - in polars.

- dataframe validator
* [x] `validate_presence_of_columns` - python.
* [x] `validate_schema` - python.
* [x] `validate_absence_of_columns` - python.

- Functions
* [x] `single_space` - rust.
* [x] `remove_all_whitespace` - rust.
* [x] `anti_trim` - rust.
* [x] `remove_non_word_characters` - rust.
* [x] `multi_equals` - rust.
* `week_start_date` - might be covered by `polars_xdt`.
* `week_end_date` see `week_start_date`.
* [x] `approx_equal` - rust.
* [ ] `array_choice` - rust. (interesting one, will be some way to do seed with a crate.)
* `business_days_between` - covered by `workday_count` in `polars_xdt`.
* `uuid5` - can cover in `faux_lars`.
* [x] `is_falsy` - rust.
* [x] `is_truthy` - rust.
* [x] `is_false` - rust.
* [x] `is_true` - rust.
* [x] `is_null_or_blank` - rust.
* [x] `null_between` - rust.

### math(s)
* `rand_laplace` - generate random numbers with `Laplace(mu, beta)` - put into `faux_lars` if is worthwhile.
* [x] `div_or_else` - rust. Question - what about the other edge cases of `IEEE-754` - dividing by a minus number. Numerator is 0 etc. Can just decide/pass arg.

### Schema Helpers
* [x] `print_schema_as_code` - python.
* `schema_from_csv` - python. Will not implement: should not be assigning schema with a csv. I have seen all sorts of nightmarish 'mapping'/'schema' files at work and I have seen enough to be convinced it is bad practice. code == code.
* [x] `complex_fields` - python.

### Split columns
* `split_col` - `quinn` function expects only one delimiter, which may not be true... I prefer how `split` polars does it, where it returns an array column. Propose not implement, or add but with an index position argument.

### Transformations
* `with_columns_renamed` - covered by `polars.DataFrame|LazyFrame.rename` in polars.
* `with_some_columns_renamed` see `with_columns_renamed`.
* [x] `snake_case_col_names` python. Needs more robust tests than `quinn`.
* `sort_columns` I think this is covered by polars sort?
* [x] `flatten_struct` python.
* `flatten_map` There is no `map` type in polars.
* `flatten_dataframe` Could possibly not implement, I don't think this handles things like deeply nested structs; less robust than it sounds. Nested structs exist in pyspark but I'm not sure in polars...
