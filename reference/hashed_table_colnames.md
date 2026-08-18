# Column names in hashed tables

A hashed table includes a column that contains a string identifying the
database table in which the data are stored. This object stores the name
of that column.

## Usage

``` r
hashed_table_colnames
```

## Format

A named list with 4 entries of hashed table column names.

## Examples

``` r
hashed_table_colnames
#> $db_table_name
#> [1] "DBTableName"
#> 
#> $nested_colname
#> [1] "NestedData"
#> 
#> $nested_hash_colname
#> [1] "NestedDataHash"
#> 
#> $tar_group_colname
#> [1] "tar_group"
#> 
```
