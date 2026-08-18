# Execute the compress function on a database table

**\[deprecated\]**

## Usage

``` r
compress_rows(
  db_table_name,
  valid_from_version_colname = PFUPipelineTools::dataset_info$valid_from_version_colname,
  valid_to_version_colname = PFUPipelineTools::dataset_info$valid_to_version_colname,
  conn
)
```

## Arguments

- db_table_name:

  The name of the table to be compressed.

- valid_from_version_colname, valid_to_version_colname:

  Column names in `db_table_name`. Defaults are
  `PFUPipelineTools::dataset_info$valid_from_version_colname` and
  `PFUPipelineTools::dataset_info$valid_to_version_colname`.

- conn:

  A connection to the database in which tables are to be compressed.

## Value

The number of rows affected by the call to `compress` `db_table_name`,
which should always be `0`.

## Details

During the execution of the pipeline, we compress the rows of a table if
identical data exist for multiple versions. The `compress` function
performs this task in the database. This `R` function is a wrapper that
executes `compress` in the database.

## See also

[`install_compress_function()`](https://energyeconomydecoupling.github.io/PFUPipelineTools/reference/install_compress_function.md),
[`remove_compress_function()`](https://energyeconomydecoupling.github.io/PFUPipelineTools/reference/remove_compress_function.md)
