# Encode and decode `matsindf` data frames for storage in a database

The CL-PFU database enables storage of `matsindf` data frames by
encoding matrix values in triplet format. These functions perform
encoding and decoding of `matsindf` data frames. `encode_matsindf()` and
`decode_matsindf()` are inverses of each other.

## Usage

``` r
decode_matsindf(
  .encoded,
  index_map,
  rctypes,
  wide_by_matrices = TRUE,
  matrix_class = c("matrix", "Matrix"),
  matname = PFUPipelineTools::mat_meta_cols$matname,
  matval = PFUPipelineTools::mat_meta_cols$matval,
  row_index_colname = PFUPipelineTools::mat_colnames$i,
  col_index_colname = PFUPipelineTools::mat_colnames$j,
  value_colname = PFUPipelineTools::mat_colnames$value,
  rowtype_colname = PFUPipelineTools::mat_meta_cols$rowtype,
  coltype_colname = PFUPipelineTools::mat_meta_cols$coltype
)

encode_matsindf(
  .matsindf,
  index_map = magrittr::set_names(list(industry_index_map, product_index_map),
    c(IEATools::row_col_types$industry, IEATools::row_col_types$product)),
  industry_index_map,
  product_index_map,
  retain_zero_structure = FALSE,
  matname = PFUPipelineTools::mat_meta_cols$matname,
  matval = PFUPipelineTools::mat_meta_cols$matval,
  row_index_colname = PFUPipelineTools::mat_colnames$i,
  col_index_colname = PFUPipelineTools::mat_colnames$j,
  value_colname = PFUPipelineTools::mat_colnames$value
)
```

## Arguments

- .encoded:

  A data frame of matrices in triplet form whose matrices are to be
  decoded.

- index_map:

  A list of two or more index map data frames. Default is
  `list(Industry = industry_index_map, Product = product_index_map)`.

- rctypes:

  A data frame of row and column types.

- wide_by_matrices:

  A boolean that tells whether to
  [`tidyr::pivot_wider()`](https://tidyr.tidyverse.org/reference/pivot_wider.html)
  the results. Default is `TRUE`.

- matrix_class:

  The class of matrices to be created by `decode_matsindf()`. One of
  "matrix" (the default and `R`'s native matrix class) or "Matrix" (for
  sparse matrices).

- matname:

  The name of the column in `.matsindf` that contains matrix names.
  Default is "matname".

- matval:

  The name of the column in `.matsindf` that contains matrix values.
  Default is "matval".

- row_index_colname:

  The name of the row index column in `.encoded`. Default is "i".

- col_index_colname:

  The name of the column index column in `.encoded`. Default is "j".

- value_colname:

  The name of the value column. Default is "value".

- rowtype_colname, coltype_colname:

  Names of `rowtype` and `coltype` columns.

- .matsindf:

  A matsindf data frame whose matrices are to be encoded.

- industry_index_map, product_index_map:

  Optional data frames with two columns providing the mapping between
  row and column indices and row and column names. See details.

- retain_zero_structure:

  A boolean that tells whether to retain the structure of zero matrices
  when creating triplets. Default is `FALSE`. See details.

## Value

For `encode_matsindf()`, a version of `.matsindf` with matrices in
triplet form, appropriate for insertion into a database. For
`decode_matsindf()`, a version of `.encoded` appropriate for in-memory
analysis and calculations.

## Details

`index_map` must be an unnamed list of two data frames or a named list
of two or more data frames.

- If an unnamed list of exactly two data frames, each data frame must
  have only an integer column and a character column. The first data
  frame of `index_map` is interpreted as the mapping between row names
  and row indices and the second data frame of `index_map` is
  interpreted as the mapping between column names and column indices.

- If a named list of two or more data frames, the names of `index_map`
  are interpreted as row and column types, with named data frames
  applied as the mapping for the associated row or column type. For
  example the data frame named "Industry" would be applied to the
  dimension (row or column) with an "Industry" type. When both row and
  column have "Industry" type, the "Industry" mapping is applied to
  both. When sending named data frames in `index_map`, matrices to be
  encoded must have both a row type and a column type. If an appropriate
  mapping cannot be found in `index_map`, an error is raised. Both
  matching data frames must have only an integer column and a character
  column.

`.matsindf` can be (a) wide by matrices, with matrix names as column
names or (b) tidy, with `matname` and `matval` columns.

If `.matsindf` does not contain any matrix columns, `.matsindf` is
returned unchanged.

If `.encoded` does not contain a `matname` column, `.encoded` is
returned unchanged.

By default, `encode_matsindf()` will return zero-row data frames when
encoding zero matrices. Set `retain_zero_structure = TRUE` to return all
entries in zero matrices.

All of `matname`, `row_index_colname`, `col_index_colname`, and
`val_colname` must be present in `.encoded`. If not, `.encoded` is
returned unmodified.
