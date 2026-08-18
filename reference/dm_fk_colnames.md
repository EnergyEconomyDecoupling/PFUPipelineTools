# Column names in foreign key tables from `dm`

`dm_get_all_fks()` from the package `dm` returns a data frame with
several columns. This constant gives those names as a string list to
document their meaning and to guard against future changes.

## Usage

``` r
dm_fk_colnames
```

## Format

A string list with 5 entries.

- child_table:

  The name of a string column that tells child table names.

- child_fk_cols:

  The name of a string column that identifies foreign key columns in the
  child table.

- parent_table:

  The name of a string column that identifies the table where the
  child's foreign key is defined.

- parent_key_cols:

  The name of a string column that tells which column in the parent
  table contains the definition of the foreign key.

- on_delete:

  Tells what will happen when the foreign key column is deleted.

## Details

In this context, a "parent" table contains the foreign key definition
and its values. A "child" table contains at least one foreign column
that references a column in the parent table.

## Examples

``` r
dm_fk_colnames
#> $child_table
#> [1] "child_table"
#> 
#> $child_fk_cols
#> [1] "child_fk_cols"
#> 
#> $parent_table
#> [1] "parent_table"
#> 
#> $parent_key_cols
#> [1] "parent_key_cols"
#> 
#> $on_delete
#> [1] "on_delete"
#> 
```
