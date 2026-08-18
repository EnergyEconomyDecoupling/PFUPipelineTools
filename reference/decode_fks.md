# Decode keys in a database table

When querying a table from a database, it is helpful to decode the
primary and foreign keys in the data frame, i.e. to translate from keys
to values. This function provides that service.

## Usage

``` r
decode_fks(
  .df = NULL,
  db_table_name,
  collect = FALSE,
  conn = NULL,
  schema = schema_from_conn(conn),
  fk_parent_tables = get_all_fk_tables(conn = conn, schema = schema),
  .child_table = PFUPipelineTools::dm_fk_colnames$child_table,
  .child_fk_cols = PFUPipelineTools::dm_fk_colnames$child_fk_cols,
  .parent_key_cols = PFUPipelineTools::dm_fk_colnames$parent_key_cols,
  .pk_suffix = PFUPipelineTools::key_col_info$pk_suffix,
  .y_joining_suffix = ".y"
)
```

## Arguments

- .df:

  A data frame whose foreign keys are to be decoded. Default is `NULL`,
  meaning that `.df` should be downloaded from `db_table_name` at `conn`
  before decoding foreign keys.

- db_table_name:

  The string name of the database table where `.df` is to be uploaded.

- collect:

  A boolean that tells whether to download the decoded table (returning
  an in-memory data frame produced by calling
  [`dplyr::collect()`](https://dplyr.tidyverse.org/reference/compute.html))
  or a `tbl` (a reference to a database query to be executed by
  [`dplyr::collect()`](https://dplyr.tidyverse.org/reference/compute.html)).
  Default is `FALSE`. Applies only when `.df` is `NULL`. Note that to
  prevent downloads from `conn`, supply values for `schema` and
  `fk_parent_tables`, whose default values will download both a `dm`
  object and the foreign key parent tables from the database at `conn`.

- conn:

  An optional database connection. Necessary only for the default values
  of `.df`, `schema`, and `fk_parent_tables`. Also necessary if
  `collect = TRUE`. Default is `NULL`.

- schema:

  The data model (`dm` object) for the database in `conn`. See details.

- fk_parent_tables:

  A named list of all parent tables for the foreign keys in
  `db_table_name`. See details.

- .child_table, .child_fk_cols, .parent_key_cols:

  See
  [`PFUPipelineTools::dm_fk_colnames`](https://energyeconomydecoupling.github.io/PFUPipelineTools/reference/dm_fk_colnames.md).

- .pk_suffix:

  See
  [`PFUPipelineTools::key_col_info`](https://energyeconomydecoupling.github.io/PFUPipelineTools/reference/key_col_info.md).

- .y_joining_suffix:

  The y column name suffix for `left_join()`. Default is ".y".

## Value

A version of `.df` with integer keys replaced by key values.

## Details

`schema` is a data model (`dm` object) for the CL-PFU database. It can
be obtained from calling
[`schema_from_conn()`](https://energyeconomydecoupling.github.io/PFUPipelineTools/reference/schema_from_conn.md).
The default is `schema_from_conn(conn = conn)`, which downloads the `dm`
object from `conn`. To save time, pre-compute the `dm` object and supply
in the `schema` argument.

`fk_parent_tables` is a named list of tables, some of which are foreign
key (fk) parent tables for `db_table_name` containing the mapping
between fk values (usually strings) and fk keys (usually integers).
`fk_parent_tables` is treated as a store from which foreign key tables
are retrieved by name when needed. An appropriate value for
`fk_parent_tables` can be obtained from
[`get_all_fk_tables()`](https://energyeconomydecoupling.github.io/PFUPipelineTools/reference/get_all_fk_tables.md).

There are two three ways to use this function.

Offline: supply a value for `.df` and values for `schema` and
`fk_parent_tables`. In offline mode, `conn` is not needed and no
connection to a database at `conn` will be made.

Download: accept the default value for `.df` (`NULL`) and supply a
`conn`. In download mode, `db_table_name` will be downloaded from the
database at `conn`.

Fast download: supply values for all of `conn`, `schema` and
`fk_parent_tables`. `schema` and `fk_parent_tables()` can be obtained by
calling `schema_from_conn(conn = conn)` and
`get_all_fk_tables(conn = conn, schema = schema)`, respectively. In fast
download mode, execution time is reduced, because the database at `conn`
has already been queried for `schema` and `fk_parent_tables`. Fast
download mode quickens multiple downloads to the same database.

## See also

[`encode_fks()`](https://energyeconomydecoupling.github.io/PFUPipelineTools/reference/encode_fks.md)
for the reverse operation.
