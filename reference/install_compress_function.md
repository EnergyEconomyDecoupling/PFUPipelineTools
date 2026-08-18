# Install the `compress` function to a database

**\[deprecated\]**

## Usage

``` r
install_compress_function(
  conn,
  compress_func_string = readr::read_file(system.file("extdata", "compress.sql", package
    = "PFUPipelineTools"))
)
```

## Arguments

- conn:

  A connection to the database into which the `compress` function should
  be added.

- compress_func_string:

  The compress function. Default is
  `readr::read_file(file.path("data-raw", "compress.sql")`.

## Value

The number of rows affected by adding the function to the database at
`conn`, which should always be `0`.

## Details

When running the pipeline, we compress identical rows of the table using
the version columns. This plpgsql function in `compress_func_string`
does that work for us. This function adds the `compress` function to the
database at `conn`.

If `compress` is already installed into the the database at `conn`, only
the user who previously installed `compress` can update it.

To remove `compress`, see
[`remove_compress_function()`](https://energyeconomydecoupling.github.io/PFUPipelineTools/reference/remove_compress_function.md).

## See also

[`remove_compress_function()`](https://energyeconomydecoupling.github.io/PFUPipelineTools/reference/remove_compress_function.md),
[`compress_rows()`](https://energyeconomydecoupling.github.io/PFUPipelineTools/reference/compress_rows.md)
