# Self-name a list

When using the native pipe operator, it is sometimes convenient to
self-name a vector or list, especially for later use in
[`lapply()`](https://rdrr.io/r/base/lapply.html) and friends.

## Usage

``` r
self_name(x)
```

## Arguments

- x:

  The character vector or list of character strings to be self-named.

## Value

A self-named version of `x`.

## Details

Internally, this function calls `magrittr::set_names(x, x)`.

## Examples

``` r
c("a", "b", "c") |>
  self_name()
#>   a   b   c 
#> "a" "b" "c" 
list("A", "B", "C") |>
  self_name()
#> $A
#> [1] "A"
#> 
#> $B
#> [1] "B"
#> 
#> $C
#> [1] "C"
#> 
```
