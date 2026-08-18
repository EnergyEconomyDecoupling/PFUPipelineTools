# Create a data model from an Excel schema table

This function returns a `dm` object suitable for future uploading to a
database.

## Usage

``` r
schema_dm(
  schema_table,
  pk_suffix = PFUPipelineTools::key_col_info$pk_suffix,
  .tablename = PFUPipelineTools::schema_table_colnames$tablename,
  .colname = PFUPipelineTools::schema_table_colnames$colname,
  .is_pk = PFUPipelineTools::schema_table_colnames$is_pk,
  .coldatatype = PFUPipelineTools::schema_table_colnames$coldatatype,
  .fk_table = PFUPipelineTools::schema_table_colnames$fk_table,
  .fk_colname = PFUPipelineTools::schema_table_colnames$fk_colname,
  .pk_cols = ".pk_cols"
)
```

## Arguments

- schema_table:

  A schema table, typically the output of
  [`load_schema_table()`](https://energyeconomydecoupling.github.io/PFUPipelineTools/reference/load_schema_table.md).

- pk_suffix:

  The suffix for primary keys. Default is "\_ID".

- .tablename, .colname, .is_pk, .coldatatype, .fk_table, .fk_colname:

  See
  [`PFUPipelineTools::schema_table_colnames`](https://energyeconomydecoupling.github.io/PFUPipelineTools/reference/schema_table_colnames.md).

- .pk_cols:

  Column names used internally.

## Value

A `dm` object created from `schema_table`.

## Details

`schema_table` is assumed to be a data frame with the following columns:

- `.tablename`: gives table names in the database.

- `.colname`: gives column names in each table; if suffixed with
  `pk_suffix`, interpreted as a primary key column.

- `.is_pk`: tells if `.colname` is a primary key for `.table`.

- `.coldatatype`: gives the data type for the column, one of "int",
  "boolean", "text", or "double precision".

- `.fk_table`: gives a table in which the foreign key can be found

- `.fk_colname`: gives the column name in fk.table where the foreign key
  can be found

## Examples

``` r
st <- tibble::tribble(~TableName, ~Colname, ~IsPK, ~ColDataType, ~FKTable, ~FKColname,
                      "Country", "CountryID", TRUE, "text", "NA", "NA",
                      "Country", "Country", FALSE, "text", "NA", "NA",
                      "Country", "Description", FALSE, "text", "NA", "NA")
schema_dm(st)
#> Error: .onLoad failed in loadNamespace() for 'dm', details:
#>   call: (function (packages, top_level_fun, use = TRUE) 
#>   error: The packages "DiagrammeR" and "DiagrammeRsvg" are required to use
#> `register_pkgdown_methods()`.
```
