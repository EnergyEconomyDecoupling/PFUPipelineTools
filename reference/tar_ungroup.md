# Ungroups and removes tar_group column from a data frame

The
[`tarchetypes::tar_group_by()`](https://docs.ropensci.org/tarchetypes/reference/tar_group_by.html)
function adds a column named "tar_group". This function ungroups and
removes the special column and optionally removes `dplyr` groups, too.

## Usage

``` r
tar_ungroup(.df, tar_group_colname = "tar_group", ungroup = TRUE)
```

## Arguments

- .df:

  The data frame to have its `targets` grouping removed.

- tar_group_colname:

  The name of the grouping column. Default is "tar_group".

- ungroup:

  A boolean that tells whether to ungroup (in the `dplyr` sense, not the
  `targets` sense) `.df`. Default is `TRUE`.

## Value

A modified version of `.df`.
