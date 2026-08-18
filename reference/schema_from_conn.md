# Get the database schema (a `dm` object) from a connection

It is helpful to know the actual schema in use by a database at a
connection. This function provides this information. It is a thin
wrapper around `dm_from_con()` from the package `dm` that enables
building the documentation website.

## Usage

``` r
schema_from_conn(conn = NULL, table_names = NULL, learn_keys = TRUE)
```

## Arguments

- conn:

  A `DBI::DBIConnection`.

- table_names:

  A character vector of the names of the tables to include. `NULL` (the
  default) means return all tables in `conn`.

- learn_keys:

  A boolean that tells whether to include the definition of primary and
  final keys in the return value. Default is `TRUE`.

## Value

A `dm` object.
