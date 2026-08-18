# Download a data frame based on its `pl_hash()`

If `hashed_table` has `0` rows, `NULL` is returned.

## Usage

``` r
pl_collect_from_hash(
  hashed_table,
  version_string = NULL,
  decode_fks = TRUE,
  retain_table_name_col = FALSE,
  set_tar_group = TRUE,
  decode_matsindf = TRUE,
  matrix_class = c("Matrix", "matrix"),
  tar_group_colname = PFUPipelineTools::hashed_table_colnames$tar_group_colname,
  matname_colname = PFUPipelineTools::mat_meta_cols$matname,
  matval_colname = PFUPipelineTools::mat_meta_cols$matval,
  valid_from_version_colname = PFUPipelineTools::dataset_info$valid_from_version_colname,
  valid_to_version_colname = PFUPipelineTools::dataset_info$valid_to_version_colname,
  conn,
  schema = schema_from_conn(conn = conn),
  fk_parent_tables = get_all_fk_tables(conn = conn, schema = schema),
  index_map_name = "Index",
  index_map = fk_parent_tables[[index_map_name]],
  rctype_table_name = "matnameRCType",
  rctypes = decode_fks(db_table_name = rctype_table_name, collect = TRUE, conn = conn,
    schema = schema, fk_parent_tables = fk_parent_tables),
  .table_name_col = PFUPipelineTools::hashed_table_colnames$db_table_name,
  .nested_hash_col = PFUPipelineTools::hashed_table_colnames$nested_hash_colname
)
```

## Arguments

- hashed_table:

  A table created by
  [`pl_hash()`](https://energyeconomydecoupling.github.io/PFUPipelineTools/reference/pl_hash.md).

- version_string:

  A string of length 1 indicating the version to be downloaded. `NULL`,
  the default, means to download all versions.

- decode_fks:

  A boolean that tells whether to decode foreign keys before returning.
  Default is `TRUE`.

- retain_table_name_col:

  A boolean that tells whether to retain the table name column
  (`.table_name_col`). Default is `FALSE`.

- set_tar_group:

  A boolean that tells whether to set the `tar_group_colname` column of
  the output to the same value as the input. There can be only one
  unique value in `tar_group_colname`, otherwise an error is raised.
  Default is `TRUE`.

- decode_matsindf:

  A boolean that tells whether to decode the a matsindf data frame.
  Calls
  [`decode_matsindf()`](https://energyeconomydecoupling.github.io/PFUPipelineTools/reference/encode_decode_matsindf.md)
  internally. Default is `TRUE`.

- matrix_class:

  One of "Matrix" (the default for sparse matrices) or ("matrix") for
  the native matrix form in `R`. Default is "Matrix".

- tar_group_colname:

  The name of the `tar_group` column. default is
  `PFUPipelineTools::hashed_table_colnames$tar_group_colname`.

- matname_colname, matval_colname:

  Names used for matrix names and matrix values. Defaults are
  `PFUPipelineTools::mat_meta_cols$matname` and
  `PFUPipelineTools::mat_meta_cols$matval`, respectively.

- valid_from_version_colname, valid_to_version_colname:

  Names for columns containing version information. Defaults are
  `PFUPipelineTools::dataset_info$valid_from_version` and
  `PFUPipelineTools::dataset_info$valid_to_version`, respectively.

- conn:

  The database connection.

- schema:

  The database schema (a `dm` object). Default calls
  [`schema_from_conn()`](https://energyeconomydecoupling.github.io/PFUPipelineTools/reference/schema_from_conn.md),
  but you can supply a pre-computed schema for speed. Needed only when
  `decode_fks = TRUE` (the default). If foreign keys are not being
  decoded, setting `NULL` may improve speed.

- fk_parent_tables:

  Foreign key parent tables to assist decoding foreign keys (when
  `decode_fks = TRUE`, the default). Default calls
  [`get_all_fk_tables()`](https://energyeconomydecoupling.github.io/PFUPipelineTools/reference/get_all_fk_tables.md).
  Needed only when `decode_fks = TRUE` (the default). If foreign keys
  are not being decoded, setting to `NULL` may improve speed.

- index_map_name:

  The name of the index map. Default is "Index".

- index_map:

  A list of data frames to assist with decoding matrices. Passed to
  [`decode_matsindf()`](https://energyeconomydecoupling.github.io/PFUPipelineTools/reference/encode_decode_matsindf.md)
  when `decode_matsindf` is `TRUE` but otherwise not needed. Default is
  `fk_parent_tables[[index_map_name]]`.

- rctype_table_name:

  The name of the row and column types.

- rctypes:

  A data frame of row and column types. Passed to
  [`decode_matsindf()`](https://energyeconomydecoupling.github.io/PFUPipelineTools/reference/encode_decode_matsindf.md)
  when `decode_matsindf` is `TRUE` but otherwise not needed. Default
  calls
  [`decode_fks()`](https://energyeconomydecoupling.github.io/PFUPipelineTools/reference/decode_fks.md).

- .table_name_col, .nested_hash_col:

  See
  [`PFUPipelineTools::hashed_table_colnames`](https://energyeconomydecoupling.github.io/PFUPipelineTools/reference/hashed_table_colnames.md).

## Value

The downloaded data frame described by `hashed_table`.
