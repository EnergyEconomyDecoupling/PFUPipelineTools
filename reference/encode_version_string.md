# Encode a version string

This function encodes a version string, returning the integer
representation of the version string.

## Usage

``` r
encode_version_string(
  version_string,
  table_name,
  conn = NULL,
  schema = schema_from_conn(conn = conn),
  fk_parent_tables = get_all_fk_tables(conn = conn, schema = schema),
  .version_colname = PFUPipelineTools::dataset_info$valid_from_version_colname
)
```

## Arguments

- version_string:

  The version to be encoded.

- table_name:

  The name of the table in which the `version_string` is found.

- conn:

  An optional database connection. Necessary only for the default values
  of `schema` and `fk_parent_tables`. Default is `NULL`.

- schema:

  The schema for the database. Default is
  `schema_from_conn(conn = conn)`.

- fk_parent_tables:

  The foreign key parent tables. Default is
  `get_all_fk_tables(conn = conn, schema = schema)`.

- .version_colname:

  A column in which versions are provided. This column is used
  internally. Default is
  `PFUPipelineTools::dataset_info$valid_from_version_colname`.
  `PFUPipelineTools::dataset_info$valid_to_version_colname` would also
  work.

## Value

An integer which is the index for `version_string`.
