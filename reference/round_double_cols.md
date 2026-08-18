# Round double-precision columns

When uploading a data frame to a Postgres database, there are times when
numerical precision prevents compressing the table with
[`compress_rows()`](https://energyeconomydecoupling.github.io/PFUPipelineTools/reference/compress_rows.md).
This function rounds all double precision columns (but not integer
columns) in `.df` to the specified precision (`digs`) and can be called
before upserting `.df` to the database.

## Usage

``` r
round_double_cols(.df, digits = 15)
```

## Arguments

- .df:

  A data frame whose double-precision columns are to be rounded.

- digits:

  The number of digits of precision. Default is `15`, to ensure rows are
  identical for
  [`compress_rows()`](https://energyeconomydecoupling.github.io/PFUPipelineTools/reference/compress_rows.md).

## Value

A version of `.df` with double-precision columns rounded.

## Examples

``` r
data.frame(name = c("pi", "pi+1"),
  # Have to make these integers ("L")
  # so they are left alone.
  million = c(1000000L, 1000001L),
  pi = c(pi, pi+1)) |>
  round_double_cols(digits = 3)
#>   name million   pi
#> 1   pi 1000000 3.14
#> 2 pi+1 1000001 4.14
```
