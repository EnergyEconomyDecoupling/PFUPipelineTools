# Create a connection to the Mexer database

The process of creating a connection to the Mexer database is
cumbersome. This function provides a way to make connections with little
fuss and a single line of code.

## Usage

``` r
get_db_conn(dbname = "MexerDB", user, host = "mexer.site", port = 6432)

get_mexerdb_conn(user)

get_sandboxdb_conn()

get_scratchmdb_conn()

get_scratchedb_conn()

get_unit_testing_conn()
```

## Arguments

- dbname:

  The string name of a database at `host`. Default is "MexerDB" for
  `get_mexerdb_conn()`, "SandboxDB" for `get_sandboxdb_conn()`,
  "ScratchMDB" for `get_scratchmdb_conn()`, and "ScratchEDB" for
  `get_scratchedb_conn()`.

- user:

  The string username at `host`. Default is "dbcreator" for
  `get_sandboxdb_conn()`, `get_scratchmdb_conn()`, and
  `get_scratchedb_conn()` appropriate for development. The default user
  for `get_unit_testing_conn()` is "mkh2".

- host:

  The string site of the database. Default is "mexer.site".

- port:

  The integer port for the connection. Default is `6432` , the port of
  the connection pooler, for `get_mexerdb_conn()`,
  `get_sandboxdb_conn()`, `get_scratchmdb_conn()`, and
  `get_scratchedb_conn()`. Default is `5432`, the standard port for
  `get_unit_testing_conn()`.

## Value

A database connection.

## Details

Note, the password for `user` must be set in `.pgpass` or a similar
file.

The returned connection should be closed by the caller with
[`DBI::dbDisconnect()`](https://dbi.r-dbi.org/reference/dbDisconnect.html).
