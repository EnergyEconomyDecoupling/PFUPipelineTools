# Encode foreign keys in a data frame to be uploaded

In the CL-PFU pipeline, we allow data frames about to be uploaded to the
database to have foreign key values (usually strings) instead of foreign
keys (integers) in foreign key columns. This function translates the fk
values to fk keys.

## Usage

``` r
encode_fks(
  .df,
  db_table_name,
  conn = NULL,
  schema = schema_from_conn(conn),
  fk_parent_tables = get_all_fk_tables(conn = conn, schema = schema),
  .child_table = PFUPipelineTools::dm_fk_colnames$child_table,
  .child_fk_cols = PFUPipelineTools::dm_fk_colnames$child_fk_cols,
  .parent_table = PFUPipelineTools::dm_fk_colnames$parent_table,
  .parent_key_cols = PFUPipelineTools::dm_fk_colnames$parent_key_cols,
  .pk_suffix = PFUPipelineTools::key_col_info$pk_suffix
)
```

## Arguments

- .df:

  The data frame about to be uploaded.

- db_table_name:

  The string name of the database table where `.df` is to be uploaded.

- conn:

  An optional database connection. Necessary only for the default values
  of `schema` and `fk_parent_tables`. Default is `NULL`.

- schema:

  The data model (`dm` object) for the database in `conn`. See details.

- fk_parent_tables:

  A named list of all parent tables for the foreign keys in
  `db_table_name`. See details.

- .child_table, .child_fk_cols, .parent_table, .parent_key_cols:

  See
  [`PFUPipelineTools::dm_fk_colnames`](https://energyeconomydecoupling.github.io/PFUPipelineTools/reference/dm_fk_colnames.md).

- .pk_suffix:

  See
  [`PFUPipelineTools::key_col_info`](https://energyeconomydecoupling.github.io/PFUPipelineTools/reference/key_col_info.md).

## Value

A version of `.df` with fk values (often strings) replaced by fk keys
(integers).

## Details

`schema` is a data model (`dm` object) for the CL-PFU database. It can
be obtained from calling
[`schema_from_conn()`](https://energyeconomydecoupling.github.io/PFUPipelineTools/reference/schema_from_conn.md).

`fk_parent_tables` is a named list of tables, some of which are fk
parent tables containing the mapping between fk values (usually strings)
and fk keys (usually integers) for `db_table_name`. `fk_parent_tables`
is treated as a store from which foreign key tables are retrieved by
name when needed. An appropriate value for `fk_parent_tables` can be
obtained from
[`get_all_fk_tables()`](https://energyeconomydecoupling.github.io/PFUPipelineTools/reference/get_all_fk_tables.md).

If `.df` contains no foreign key columns, `.df` is returned unmodified.

## See also

[`decode_fks()`](https://energyeconomydecoupling.github.io/PFUPipelineTools/reference/decode_fks.md)
for the inverse operation, albeit for all keys, primary and foreign.
