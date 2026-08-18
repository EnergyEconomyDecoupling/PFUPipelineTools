# Update a schema table in a database.

It is sometimes necessary to update a schema table in the database. The
need can arise when additional rows are added to the Index table, for
example. This function helps with that process.

## Usage

``` r
update_schema_table(
  dbname,
  db_table_name,
  input_data_version,
  project_path = file.path("~", "OneDrive", "OneDrive - University of Leeds",
    "Fellowship 1960-2015 PFU database research"),
  input_data_path = file.path(project_path, "Input Data", "CL-PFU Data",
    input_data_version),
  schema_path = file.path(input_data_path, "SchemaAndFKTables.xlsx"),
  schema_sheet = "SchemaTable",
  table_colname = "TableName",
  is_pk_colname = "IsPK",
  colname_colname = "Colname",
  drv = RPostgres::Postgres(),
  host = "mexer.site",
  port = 6432,
  user = "dbcreator",
  conn = DBI::dbConnect(drv = drv, dbname = dbname, host = host, port = port, user =
    user)
)
```

## Arguments

- dbname:

  The name of the database in which the table should be updated.

- db_table_name:

  The name of the table to update.

- input_data_version:

  A string representing the version of input data to be used.

- project_path:

  The path to the schema excel file. Default is the OneDrive folder for
  this project.

- input_data_path:

  The path from `project_path` to the input data folder.

- schema_path:

  The path from `input_data_path` to the schema file.

- schema_sheet:

  The name of the schema sheet in the schema file. Default is "Schema".

- table_colname, is_pk_colname, colname_colname:

  Names of the table, isPK, and colname columns in the schema table.
  Defaults are "TableName", "IsPK", and "Colname", respectively.

- drv:

  The database driver to be used. Default is
  [`RPostgres::Postgres()`](https://rpostgres.r-dbi.org/reference/Postgres.html).

- host:

  The host for the database. Default is "mexer.site".

- port:

  The port for accessing the database. Default is `6432`.

- user:

  The user for the database. Default is "dbcreator".

- conn:

  The connection to the database. Default is created from `drv`,
  `dbname`, `host`, `port`, and `user` arguments.

## Value

`TRUE` if successful
