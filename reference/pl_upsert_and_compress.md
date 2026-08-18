# Upsert and compress rows from a data frame

Upserts (inserts or updates, depending on whether the private keys in
`.df` already exist in `db_table_name`) `.df` into `db_table_name` at
`conn`.

## Usage

``` r
pl_upsert_and_compress(
  .df,
  conn,
  db_table_name = NULL,
  version_string = NULL,
  additional_hash_group_cols = NULL,
  usual_hash_group_cols = PFUPipelineTools::usual_hash_group_cols,
  keep_single_unique_cols = TRUE,
  in_place = FALSE,
  encode_fks = TRUE,
  compress = TRUE,
  tol = 1e-06,
  round_double_columns = FALSE,
  digits = 15,
  index_map_name = "Index",
  index_map = fk_parent_tables[[index_map_name]],
  retain_zero_structure = FALSE,
  schema = schema_from_conn(conn),
  fk_parent_tables = get_all_fk_tables(conn = conn, schema = schema, collect = TRUE),
  .db_table_name = PFUPipelineTools::hashed_table_colnames$db_table_name,
  .pk_col = PFUPipelineTools::dm_pk_colnames$pk_col,
  .algo = "md5",
  mat_colnames = unlist(PFUPipelineTools::mat_colnames),
  valid_from_version_colname = PFUPipelineTools::dataset_info$valid_from_version_colname,
  valid_to_version_colname = PFUPipelineTools::dataset_info$valid_to_version_colname,
  value_colname = PFUPipelineTools::mat_colnames$value,
  what_to_do_colname = PFUPipelineTools::dataset_info$what_to_do,
  current_version_string = PFUPipelineTools::version_info$current_version_string,
  current_version_int = PFUPipelineTools::version_info$current_version_int
)
```

## Arguments

- .df:

  The data frame to be upserted (and compressed by default).

- conn:

  A connection to the CL-PFU database.

- db_table_name:

  A string identifying the destination for `.df` in `conn`, i.e. the
  name of a remote database table. Default is `NULL`, meaning that the
  value for this argument will be taken from the `.db_table_name` column
  of `.df`.

- version_string:

  An optional string that tells the version of the database being
  updated. See details. Default is `NULL`, meaning version information
  should be obtained from the `ValidFromVersion` and `ValidToVersion`
  columns of `.df`.

- additional_hash_group_cols:

  A vector or list of additional columns by which `.df` will be grouped
  before hashing and, therefore, appear in the output. Default is
  `NULL`. Passed to
  [`pl_hash()`](https://energyeconomydecoupling.github.io/PFUPipelineTools/reference/pl_hash.md).

- usual_hash_group_cols:

  A vector of columns by which `.df` will be grouped before hashing and,
  therefore, appear in the output. Default is
  `PFUPipelineTools::additional_hash_group_cols` but can be set to
  `NULL` to disable. Passed to
  [`pl_hash()`](https://energyeconomydecoupling.github.io/PFUPipelineTools/reference/pl_hash.md).

- keep_single_unique_cols:

  A boolean that tells whether to keep columns with a single unique
  value in the output. Default is `TRUE`. Passed to
  [`pl_hash()`](https://energyeconomydecoupling.github.io/PFUPipelineTools/reference/pl_hash.md).

- in_place:

  A boolean that tells whether to modify the database at `conn`. Default
  is `FALSE`, which is helpful if you want to chain several requests.

- encode_fks:

  A boolean that tells whether to code foreign keys in `.df` before
  upserting to `conn`. Default is `TRUE`.

- compress:

  A boolean that tells whether to compress `db_table_name` in the
  database after uploading. Default is `TRUE`.

- tol:

  The tolerance within which a local value will be assumed same as the
  remote value. This value is passed to
  [`compress_helper()`](https://energyeconomydecoupling.github.io/PFUPipelineTools/reference/compress_helper.md).
  Default is `1e-6`.

- round_double_columns:

  A boolean that tells whether to round double-precision columns in
  `.df`. Default is `FALSE`.

- digits:

  An integer that tells the number of significant digits. `digits` has
  an effect only when `round_double_columns` is `TRUE`. Default is `15`,
  which should eliminate any numerical precision errors for
  [`compress_rows()`](https://energyeconomydecoupling.github.io/PFUPipelineTools/reference/compress_rows.md).

- index_map_name:

  The name of the table that serves as the index for row and column
  names. Default is "Index".

- index_map:

  The index map for the matrices in the database at `conn`. Default is
  `fk_parent_tables[[index_table_name]]`.

- retain_zero_structure:

  A boolean that tells whether to retain the structure of zero matrices.
  See details.

- schema:

  The data model (`dm` object) for the database in `conn`. Default is
  `dm_from_con(conn, learn_keys = TRUE)`. See details.

- fk_parent_tables:

  A named list of all parent tables for the foreign keys in
  `db_table_name`. See details.

- .db_table_name:

  The name of the table name column in `.df`. Default is
  `PFUPipelineTools::hashed_table_colnames$db_table_name`.

- .pk_col:

  The name of the primary key column in a primary key table. See
  [`PFUPipelineTools::dm_pk_colnames`](https://energyeconomydecoupling.github.io/PFUPipelineTools/reference/dm_pk_colnames.md).

- .algo:

  The hashing algorithm. Default is "md5".#' @param db_table_name The
  name of the table in the database at `conn` into which `.df` will be
  upserted (and compressed by default).

- mat_colnames:

  String names of columns in `.df` that contain matrix information,
  namely, rowname (or index), colname (or index), and value. Default is
  [mat_colnames](https://energyeconomydecoupling.github.io/PFUPipelineTools/reference/mat_colnames.md)
  (as a vector).

- valid_from_version_colname:

  The string name of the valid from version column. Default is
  [dataset_info](https://energyeconomydecoupling.github.io/PFUPipelineTools/reference/dataset_info.md)`$valid_from_version_colname`
  or "ValidFromVersion". Cannot be
  `PFUPipelineTools::version_info$current_version_string` or "current".

- valid_to_version_colname:

  The string name of the valid to version column. Default is
  [dataset_info](https://energyeconomydecoupling.github.io/PFUPipelineTools/reference/dataset_info.md)`$valid_to_version_colname`
  or "ValidToVersion". Cannot be
  `PFUPipelineTools::version_info$current_version_string` or "current".

- value_colname:

  The string name of the value column in `.df`. Default is
  [mat_colnames](https://energyeconomydecoupling.github.io/PFUPipelineTools/reference/mat_colnames.md)`$value`
  or "value".

- what_to_do_colname:

  The string name of a column that tells what to do with various rows of
  `.df`. This column is used internally. Default is
  [dataset_info](https://energyeconomydecoupling.github.io/PFUPipelineTools/reference/dataset_info.md)`$what_to_do`
  or "WhatToDo".

- current_version_string:

  A string that identifies the current version in the remote table.
  Default is `PFUPipelineTools::version_info$current_version_string` or
  current. It is probably a *very bad* idea to supply a different value
  from the default.

- current_version_int:

  An integer that indicates the current version in the remote table.
  Default is `PFUPipelineTools::version_info$current_version_int`
  or 2147483647. It is probably a *very bad* idea to supply a different
  value from the default.

## Value

A hash of `.df` according to `.algo`. If `.df` is `NULL` or has no rows,
`NULL` is returned.

## Details

This function decodes foreign keys (fks), when possible, assuming that
all fks are integers. If non-integers (typically, character strings) are
provided in fk columns of `.df`, the non-integers will be recoded to
their appropriate integer key values.

This function knows about CL-PFU database tables that contain matrix
information. In particular, if `.df` contains matrices, they are
expanded into row-col-val format before uploading.

The output of this function is a special data frame that contains the
following columns:

- All single-valued columns columns in `.df`, columns given in
  `additional_hash_group_cols` (default `NULL`), and columns given in
  `usual_hash_group_cols` (default
  [`PFUPipelineTools::usual_hash_group_cols`](https://energyeconomydecoupling.github.io/PFUPipelineTools/reference/usual_hash_group_cols.md)).

- Hash: A column with a hash of all non-foreign-key columns.

`schema` is a data model (`dm` object) for the database in `conn`. Its
default value (`schema_from_conn(conn)`) extracts the data model for the
database at `conn` automatically. However, if the caller already has the
data model, supplying it in the `schema` argument will save time.

`fk_parent_tables` is a named list of tables, one of which (the one
named `db_table_name`) contains the foreign keys for `db_table_name`.
`fk_parent_tables` is treated as a store from which foreign key tables
are retrieved by name when needed. The default value (which calls
[`get_all_fk_tables()`](https://energyeconomydecoupling.github.io/PFUPipelineTools/reference/get_all_fk_tables.md)
with `collect = TRUE` because decoding of foreign keys is done outboard
of the database) retrieves all possible foreign key parent tables from
`conn`, potentially a time-consuming process. For speed, pre-compute all
foreign key parent tables once (via `get_all_fk_tables(collect = TRUE)`)
and pass the list to the `fk_parent_tables` argument of this function.

The user in `conn` must have write access to the database.

By default, `pl_upsert_and_compress()` will delete all zero entries in
matrices before upserting. But for some countries and years, that could
result in missing matrices, such as **U_EIOU**. Set
`retain_zero_structure = TRUE` to preserve all entries in a zero matrix.

Optionally (and by default), this function compresses the remote data in
`db_table_name` by setting appropriate values in the `ValidFromVersion`
and `ValidToVersion` columns.

This function assumes the `ValidToVersion` column in the remote contains
`PFUPipelineTools::version_info$current_version_int` or 2147483647 (the
largest possible integer in both PostgreSQL and `R`) for the most
current version of the data.

There are only a few possibilities for rows of data in the local data
frame and the remote database table:

- Same: Rows in the local data frame match rows in the remote database
  table for all foreign key columns except ValidFromVersion and
  ValidToVersion and within `tol` for the `value` column. In this case,
  there is nothing to be done, because the `ValidToVersion` column in
  the remote database table should already be `2147483647`.

- Old/New:

  - Remote (Old): Rows in the remote database table with foreign key
    columns (except `ValidFromVersion` and `ValidToVersion`) that have
    no match in the local data frame. In this case, we change the
    `ValidToVersion` column in the the unmatched rows of the remote
    database table to `working_version - 1`.

  - Local (New): Rows in the local data frame with foreign key columns
    (except `ValidFromVersion` and `ValidToVersion`) that have no match
    in the remote database table. In this case, we change the
    `ValidToVersion` column of the unmatched rows in the local data
    frame to `2147483647` and insert into the remote database table.

- Updated: Rows in the local data frame match rows in the remote
  database table for all foreign key columns except `ValidFromVersion`
  and `ValidToVersion` but outside of `tol` for the `value` column. In
  this case, we change the `ValidToVersion` column in the the unmatched
  rows of the remote database table to `working_version - 1`. We also
  change the `ValidToVersion` column of the unmatched rows in the local
  data frame to `2147483647` and insert into the remote database table.

Note that `mat_colnames` is used to discriminate data and metadata
columns. Columns of `.df` (local) and `db_table_name` (remote) in
`mat_colnames` are considered to be data columns. All other columns are
considered to be metadata columns. The calling function should supply
the complete data in `.df` for each unique combination metadata column
values.

`version_string` is an optional argument that tells the version of the
database being uploaded. The default is `NULL`, meaning that version
information should be obtained from the the `ValidFromVersion` and
`ValidToVersion` columns of `.df`. If `version_string` is specified, it
must be a string vector of length 1.

An error will occur if either the `ValidFromVersion` or the
`ValidToVersion` column of `.df` contains
`PFUPipelineTools::version_info$current_version_string` or "current".
