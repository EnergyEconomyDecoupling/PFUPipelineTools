# Get a named list of foreign key tables

For a database whose tables have many foreign keys that point to primary
key tables, it is helpful to have a list of those foreign key tables for
inboard and outboard processing purposes, especially when converting
strings to integer keys. This function extracts a named list of data
frames, where each data frame is a foreign key table in the database.
The data frame items in the list are named by the names of the tables in
the database at `conn`.

## Usage

``` r
get_all_fk_tables(conn, schema = schema_from_conn(conn), collect = FALSE)
```

## Arguments

- conn:

  A connection to a database.

- schema:

  The data model (`dm` object) for the database in `conn`. Default calls
  `schema_from_conn(conn)`. See details.

- collect:

  A boolean that tells whether to download the foreign key tables.
  Default is `FALSE` to enable inboard processing of queries.

## Value

A named list of data frames, where each data frame is a table in `conn`
that contains foreign keys and their values.

## Details

By default, the returned list contains `tbl` objects, with references to
the actual tables in `conn`. To return the actual data frames, set
`collect` to `TRUE`.

`schema` is a data model (`dm` object) for the database in `conn`. Its
default value (which calls `schema_fom_conn()`) extracts the data model
for the database at `conn` automatically. However, if the caller already
has the data model, supplying it in the `schema` argument will save some
time.
