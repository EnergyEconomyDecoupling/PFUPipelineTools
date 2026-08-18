# Set NOT NULL constraints on foreign key columns

To maintain the integrity of data in a database, it is important to
enforce NOT NULL constraints on foreign key columns. This function adds
NOT NULL constraints to every foreign key column in `schema`.

## Usage

``` r
set_not_null_constraints_on_fk_cols(
  schema,
  conn,
  .child_table = PFUPipelineTools::dm_fk_colnames$child_table,
  .child_fk_cols = PFUPipelineTools::dm_fk_colnames$child_fk_cols
)
```

## Arguments

- schema:

  A `dm` object describing the schema for the database at `conn`.

- conn:

  A database connection.

- .child_table, .child_fk_cols:

  See `PFUPpipelineTools::dm_fk_colnames`.

## Value

`NULL` silently.
