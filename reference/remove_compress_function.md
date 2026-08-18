# Remove the compress function from a database

**\[deprecated\]**

## Usage

``` r
remove_compress_function(conn)
```

## Arguments

- conn:

  A connection to the database from which the `compress` function should
  be removed.

## Value

The number of rows affected by adding the function to the database at
`conn`, which should always be `0`.

## Details

The database at `conn` may have the `compress` function installed by
`unpload_compress_function()`. This `R` function removes the `compress`
function from the database at `conn`.

The function can be removed only by the owner of the function, i.e., the
user who installed the function in the first instance.

## See also

[`install_compress_function()`](https://energyeconomydecoupling.github.io/PFUPipelineTools/reference/install_compress_function.md),
[`compress_rows()`](https://energyeconomydecoupling.github.io/PFUPipelineTools/reference/compress_rows.md)
