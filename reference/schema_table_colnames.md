# Column names in schema tables

A string list names of columns in schema tables.

## Usage

``` r
schema_table_colnames
```

## Format

A string list with 6 entries.

- tablename:

  The name of a string column that tells the database table name.

- colname:

  The name of a string column that identifies a column in `table`.

- is_pk:

  The name of a boolean column that tells whether `column` is a primary
  key.

- coldatatype:

  The name of a string column that tells the data type of `column`, such
  as "int", "text", "boolean", or "double precision".

- fk_table:

  The name of a string column that identifies the table in which a
  foreign key can be found. "NA" is a valid value for `fk_table`,
  meaning that `colname` does not have a foreign key.

- fk_colname:

  The name of a string column that identifies the name of a column in
  `fk_table` that contains values for the foreign key. "NA" is a valid
  value for `fk_colname`, meaning that `colname` does not have a foreign
  key.

## Examples

``` r
schema_table_colnames
#> $tablename
#> [1] "TableName"
#> 
#> $colname
#> [1] "Colname"
#> 
#> $is_pk
#> [1] "IsPK"
#> 
#> $coldatatype
#> [1] "ColDataType"
#> 
#> $fk_table
#> [1] "FKTable"
#> 
#> $fk_colname
#> [1] "FKColname"
#> 
```
