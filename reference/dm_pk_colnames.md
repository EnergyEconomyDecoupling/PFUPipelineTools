# Column names in primary key tables from `dm`

`dm_get_all_pks()` from the package `dm` returns a data frame with
several columns. This constant gives those names as a string list to
document their meaning and to guard against future changes.

## Usage

``` r
dm_pk_colnames
```

## Format

A string list with 3 entries.

- table:

  The name of a string column that tells table names.

- pk_col:

  The name of a string column that identifies primary key columns in
  `table`.

- autoincrement:

  The name of a boolean column that tells whether `pk_col` is
  auto-incrementing.

## Examples

``` r
dm_pk_colnames
#> $table
#> [1] "table"
#> 
#> $pk_col
#> [1] "pk_col"
#> 
#> $autoincrement
#> [1] "autoincrement"
#> 
```
