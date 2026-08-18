# A helper function that performs the upsert and compress action

A helper function that performs the upsert and compress action

## Usage

``` r
do_upsert_and_compress(
  df_to_upsert,
  valid_from_version_colname,
  valid_to_version_colname,
  matname_colname = PFUPipelineTools::mat_meta_cols$matname,
  row_colname = PFUPipelineTools::mat_colnames$row,
  col_colname = PFUPipelineTools::mat_colnames$col,
  value_colname = PFUPipelineTools::mat_colnames$value,
  set_id_cols = PFUPipelineTools::usual_hash_group_cols,
  what_to_do_colname,
  mat_colnames,
  remote_tbl,
  pk_str,
  tol,
  current_version_int,
  in_place
)
```

## Arguments

- df_to_upsert:

  The data frame to be upserted.

- valid_from_version_colname:

  The string name of the valid from version column. Cannot be
  `PFUPipelineTools::version_info$current_version_string` or "current".

- valid_to_version_colname:

  The string name of the valid to version column. Cannot be
  `PFUPipelineTools::version_info$current_version_string` or "current".

- matname_colname:

  The name of the column that contains matrix names. Default is
  [mat_meta_cols](https://energyeconomydecoupling.github.io/PFUPipelineTools/reference/mat_meta_cols.md)`$matname`
  of "matname".

- row_colname:

  The name of the column that indicates matrix row names. Default is
  [mat_colnames](https://energyeconomydecoupling.github.io/PFUPipelineTools/reference/mat_colnames.md)`$row`
  or "i".

- col_colname:

  The name of the column that indicates matrix column names. Default is
  [mat_colnames](https://energyeconomydecoupling.github.io/PFUPipelineTools/reference/mat_colnames.md)`$col`
  or "j".

- value_colname:

  The name of the column that indicates matrix values. This argument
  could have length greater than 1, in which case, all items in the
  vector are assumed to be value columns. Default is
  [mat_colnames](https://energyeconomydecoupling.github.io/PFUPipelineTools/reference/mat_colnames.md)`$value`
  or "value".

- set_id_cols:

  String names of columns that identify a complete set of data in
  df_to_upsert. This vector is used internally to identify columns by
  which to join when deciding changes from remote data. Default is
  [usual_hash_group_cols](https://energyeconomydecoupling.github.io/PFUPipelineTools/reference/usual_hash_group_cols.md).

- what_to_do_colname:

  The string name of a column that tells what to do with various rows of
  `.df`. This column is used internally.

- mat_colnames:

  String names of columns in `.df` that contain matrix information,
  namely, rowname (or index), colname (or index), and value.

- remote_tbl:

  A remote table against which `df_to_upsert` is compared.

- pk_str:

  The string for the primary key.

- tol:

  The tolerance within which a local value will be assumed same as the
  remote value. This value is passed to
  [`compress_helper()`](https://energyeconomydecoupling.github.io/PFUPipelineTools/reference/compress_helper.md).

- current_version_int:

  An integer that indicates the current version in the remote table.

- in_place:

  A boolean that tells whether to change the remote table.

## Value

Nothing useful. This function should be called for its side effect of
updating the remote table.
