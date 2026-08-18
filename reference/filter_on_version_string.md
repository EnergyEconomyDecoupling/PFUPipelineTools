# Filter a database table based on a version string.

Filtering a database table based on the version you wish to download is
a common task. In the CL-PFU database, we store data in a compressed
format where identical data points are not duplicated. Rather, they are
stored in a single row with the `ValidFromVersion` and `ValidToVersion`
columns incremented appropriately.

## Usage

``` r
filter_on_version_string(
  tbl,
  version_string,
  db_table_name,
  collect = FALSE,
  conn = NULL,
  schema = schema_from_conn(conn = conn),
  fk_parent_tables = get_all_fk_tables(conn = conn, schema = schema),
  valid_from_version_colname = PFUPipelineTools::dataset_info$valid_from_version_colname,
  valid_to_version_colname = PFUPipelineTools::dataset_info$valid_to_version_colname
)
```

## Arguments

- tbl:

  The `tbl` object that should be filtered.

- version_string:

  A vector of version strings to indicate the desired version(s).

- db_table_name:

  The name of the table to be filtered.

- collect:

  A boolean that tells whether to collect `tbl` from `conn` before
  returning. Default is `FALSE`.

- conn:

  An optional database connection. Necessary only for the default values
  of `schema` and `fk_parent_tables`. Default is `NULL`.

- schema:

  The database schema (a `dm` object). Default calls
  [`schema_from_conn()`](https://energyeconomydecoupling.github.io/PFUPipelineTools/reference/schema_from_conn.md),
  but you can supply a pre-computed schema for speed. Needed only when
  `decode_fks = TRUE` (the default). If foreign keys are not being
  decoded, setting `NULL` may improve speed.

- fk_parent_tables:

  Foreign key parent tables to assist decoding foreign keys. Default
  calls
  [`get_all_fk_tables()`](https://energyeconomydecoupling.github.io/PFUPipelineTools/reference/get_all_fk_tables.md).

- valid_from_version_colname:

  The name of the ValidFromVersion column. Default is
  `PFUPipelineTools::dataset_info$valid_from_version_colname`.

- valid_to_version_colname:

  The name of the ValidToVersion column. Default is
  `PFUPipelineTools::dataset_info$valid_to_version_colname`.

## Value

A filtered version of `tbl`.

## Details

The desired version is supplied in the `version_string` argument, which
can be a vector of any length.

In the outgoing data frame, both `valid_from_version_colname` and
`valid_to_version_colname` in the outgoing data frame will contain
identical values, namely `version_string`. This behavior avoids
ambiguity regarding the version to which each row belongs and abstracts
the complexity of the compression algorithm in the remote database. This
behavior means that asking for two or more versions (in
`version_string`) may return identical rows except for the version
columns.

If both `tbl` and `db_table_name` are provided, `db_table_name` is
ignored.
