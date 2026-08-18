# Load foreign key tables for the CL-PFU database from a spreadsheet

The SchemaAndSimpleTables.xlsx file contains a sheet that represents the
schema for the CL-PFU database. The sheet is designed to enable easy
changes to the CL-PFU database schema for successive versions of the
database. All other tabs (besides `readme_sheet`) are foreign key
tables. This function reads all foreign key tables from the
SchemaAndSimpleTables.xlsx file and returns a named list of those
tables, each in data frame format.

## Usage

``` r
load_fk_tables(
  version,
  simple_tables_path = PFUSetup::get_abs_paths(version = version)[["schema_path"]],
  readme_sheet = "README",
  schema_sheet = "SchemaTable",
  .tablename = PFUPipelineTools::schema_table_colnames$tablename,
  .colname = PFUPipelineTools::schema_table_colnames$colname,
  .coldatatype = PFUPipelineTools::schema_table_colnames$coldatatype
)
```

## Arguments

- version:

  The database version for input information.

- simple_tables_path:

  The path to the file containing simple tables. Default is
  `PFUSetup::get_abs_paths()[["schema_path"]]`

- readme_sheet:

  The name of the sheet in the file at `simple_tables_path` that
  contains readme information. Default is "README".

- schema_sheet:

  The name of the sheet in the in the file at `simple_tables_path` that
  contains schema information.\` Default is "Schema".

- .tablename, .colname, .coldatatype:

  See
  [`PFUPipelineTools::schema_table_colnames`](https://energyeconomydecoupling.github.io/PFUPipelineTools/reference/schema_table_colnames.md).

## Value

A named list of data frames, each containing a foreign key table for the
CL-PFU database.

## Details

`readme_sheet` and `schema_sheet` are ignored. All other sheets in the
file at `schema_path` are assumed to be foreign key tables that are
meant to be uploaded to the database.
