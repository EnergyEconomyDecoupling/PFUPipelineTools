# Read a CL-PFU database schema file

The SchemaAndSimpleTables.xlsx file contains a sheet that represents the
schema for the CL-PFU database. The sheet is designed to enable easy
changes to the CL-PFU database schema for successive versions of the
database. This function reads the schema table from the
SchemaAndSimpleTables.xlsx file.

## Usage

``` r
load_schema_table(
  version,
  schema_path = PFUSetup::get_abs_paths(version = version)[["schema_path"]],
  schema_sheet = "SchemaTable"
)
```

## Arguments

- version:

  The database version for input information.

- schema_path:

  The path to the schema file. Default is
  `PFUSetup::get_abs_paths()[["schema_path"]]`

- schema_sheet:

  The name of the sheet in the schema file at `schema_path` that
  contains information about tables, columns, and primary and foreign
  keys. Default is "Schema".

## Value

A `dm` object containing schema information for the CL-PFU database.

## Examples

``` r
if (FALSE) { # \dontrun{
load_schema_table(version = "v3.0")
} # }
```
