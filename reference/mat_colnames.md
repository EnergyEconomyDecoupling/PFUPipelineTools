# Unwrapped matrix column names

A string list containing names of columns for unwrapped matrices. The
CL-PFU database stores matrices in row, col, val format. These are the
names of those columns.

## Usage

``` r
mat_colnames
```

## Format

A string list with 6 entries.

- i:

  The name of the row column, namely "i".

- row:

  The name of the row column, namely "i".

- j:

  The name of the column column, namely "j".

- col:

  The name of the column column, namely "j".

- x:

  The name of the value column, namely "value".

- value:

  The name of the value column, namely "value".

## Examples

``` r
mat_colnames
#> $i
#> [1] "i"
#> 
#> $row
#> [1] "i"
#> 
#> $j
#> [1] "j"
#> 
#> $col
#> [1] "j"
#> 
#> $x
#> [1] "value"
#> 
#> $value
#> [1] "value"
#> 
```
