# PSUT matrix formation meta information column names

When forming PSUT matrices, meta information is provided in columns.
This list provides the typical names for the meta information columns
throughout the `PFUPipelineTools` package.

## Usage

``` r
mat_meta_cols
```

## Format

A string list with 6 entries.

- matname:

  The name of the column that contains matrix names.

- matval:

  The name of the column in a tidy data frame that contains matrices
  themselves.

- rowname:

  The name of the column that contains matrix row names.

- colname:

  The name of the column that contains matrix column names.

- rowtype:

  The name of the column that contains matrix row types.

- coltype:

  The name of the column that contains matrix column types.

## Examples

``` r
mat_meta_cols
#> $matname
#> [1] "matname"
#> 
#> $matval
#> [1] "matval"
#> 
#> $rowname
#> [1] "rowname"
#> 
#> $colname
#> [1] "colname"
#> 
#> $rowtype
#> [1] "rowtype"
#> 
#> $coltype
#> [1] "coltype"
#> 
```
