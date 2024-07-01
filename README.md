# harley
Polars port of PySpark's Quinn


# RoadMap
Contains:
* Proposed implementation by function.
* Proposed namespace extension criteria.

## All Functions
All functions, whether they should be implemented.

#### append if schema identical
* `append_if_schema_identical` - do not implement. `polars` append is strict on schema by default (separate namespace for diagonal append).
#### dataframe_helpers
* [*] `column_to_list` - python. (this is just polars `select`, `collect`, `to_list`, much more convenient than spark).
* [*] `two_columns_to_dictionary` - python
* `to_list_of_dictionaries` - in polars. (check)
* `show_output_to_df` - polars already has [init_repr_to_df](https://docs.pola.rs/api/python/stable/reference/api/polars.from_repr.html).
* `create_df` - in polars native.

#### dataframe validator
* [*] `validate_presence_of_columns` - python.
* [*] `validate_schema` - python.
* [*] `validate_absence_of_columns` - python.

#### Functions
* [ ] `single_space` - rust.
* [ ] `remove_all_whitespace` - rust.
* [ ] `anti_trim` - rust.
* [ ] `remove_non_word_characters` - rust.
* `exists` - Propose not implement bc don't want to allow for an arbitrary python function as the callable. This requires inefficient map which will tank performance.
* `forall` - see `exists`.
* [ ] `multi_equals` - rust.
* `week_start_date` - might be covered by `polars_xdt`.
* `week_end_date` see `week_start_date`.
* [ ] `approx_equal` - rust.
* [ ] `array_choice` - rust. (interesting one, will be some way to do seed with a crate.)
* `business_days_between` - covered by `workday_count` in `polars_xdt`.
* `uuid5` - can cover in `faux_lars`.
* [*] `is_falsy` - rust.
* [*] `is_truthy` - rust.
* [*] `is_false` - rust.
* [*] `is_true` - rust.
* `is_null_or_blank` DONE - rust.
* `is_not_in` - might be covered by native polars.
* [ ] `null_between` - rust.

### Keyword Finder
Propose skipping. Undocumented.
Wouldn't even make a very good log parser.

### math(s)
* `rand_laplace` - generate random numbers with `Laplace(mu, beta)` - put into `faux_lars` if is worthwhile.
* [ ] `div_or_else` - rust. Question - what about the other edge cases of `IEEE-754` - dividing by a minus number. Numerator is 0 etc. Can just decide/pass arg.

### Schema Helpers
* [ ] `print_schema_as_code` - python.

* `schema_from_csv` - python. Will not implement: should not be assigning schema with a csv. I have seen all sorts of nightmarish 'mapping'/'schema' files at work and I have seen enough to be convinced it is bad practice. code == code.
* [*] `complex_fields` - python.

### Split columns
* `split_col` - `quinn` function expects only one delimiter, which may not be true... I prefer how `split` polars does it, where it returns an array column. Propose not implement, or add but with an index position argument.

### Transformations
* `with_columns_renamed` - covered by `polars.DataFrame|LazyFrame.rename` in polars.
* `with_some_columns_renamed` see `with_columns_renamed`.
* `snake_case_col_names` python. DONE. Needs more robust tests than `quinn`.
* `sort_columns` I think this is covered by polars sort?
* [ ] `flatten_struct` python.
* `flatten_map` There is no `map` type in polars.
* `flatten_dataframe` Could possibly not implement, I don't think this handles things like deeply nested structs; less robust than it sounds. Nested structs exist in pyspark but I'm not sure in polars...


## Namespace registration
TODO: decide on how/whether should [extend the polars API](https://docs.pola.rs/api/python/stable/reference/api.html) for these functions. 
Should be a straightforward:
* `expr` namespace for the col transforms.
* `DataFrame` & `LazyFrame` namespace for the Frame transforms.
* `Series`?

And then, there should be an `_` for the base functions and no `_` when registered?

Some functions, such as `schema_from_csv`will be no namespace extension.
