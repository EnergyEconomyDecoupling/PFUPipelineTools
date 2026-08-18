# Upload a schema and simple tables for the CL-PFU database

When you need to start over again, you need to delete database tables,
re-define the schema, and upload simple tables. This function makes it
easy.

## Usage

``` r
pl_upload_schema_and_simple_tables(
  schema,
  simple_tables,
  conn,
  set_not_null_constraints = TRUE,
  drop_db_tables = FALSE,
  .pk_col = PFUPipelineTools::dm_pk_colnames$pk_col
)
```

## Arguments

- schema:

  A data model (a `dm` object).

- simple_tables:

  A named list of data frames with the content of tables in `conn`.

- conn:

  A `DBI` connection to a database.

- set_not_null_constraints:

  A boolean that tells whether `NOT NULL` constraints are set on foreign
  key columns in `schema`. Default is `TRUE`.

- drop_db_tables:

  If `TRUE`, all tables in conn are dropped. If a character vector, the
  names of tables to be dropped. If `FALSE` (the default), no tables are
  dropped. existing tables before uploading the new schema.

- .pk_col:

  See
  [`PFUPipelineTools::dm_pk_colnames`](https://energyeconomydecoupling.github.io/PFUPipelineTools/reference/dm_pk_colnames.md).

## Value

The remote data model

## Details

Optionally (by setting `drop_db_tables = TRUE`), deletes existing tables
in the database before uploading the schema (`schema`) and simple tables
(`simple_tables`). `drop_db_tables` is `FALSE` by default. However, it
is unlikely that this function will succeed unless `drop_db_tables` is
set `TRUE`, because uploading the data model `schema` to `conn` will
fail if the tables already exist in the database at `conn`.

`simple_tables` should not include tables with foreign keys, because the
order for uploading `simple_tables` is not guaranteed. Doing so will
avoid uploading a table with a foreign key before the parent table
containing the foreign key values has been uploaded.

`set_not_null_constraints` controls setting of `NOT NULL` constraints
for all foreign key columns in `schema`.

`conn`'s user must have superuser privileges.
