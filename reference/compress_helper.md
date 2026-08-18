# A local table compression helper function

We decide before uploading to the remote database whether rows in the
remote table can be compressed via the use of `ValidFromVersion` and
`ValidToVersion` columns. This function contains the logic for that
work.

## Usage

``` r
compress_helper(
  remote_df,
  local_df,
  valid_from_version_colname = PFUPipelineTools::dataset_info$valid_from_version_colname,
  valid_to_version_colname = PFUPipelineTools::dataset_info$valid_to_version_colname,
  value_colname = PFUPipelineTools::mat_colnames$value,
  nam = "nam",
  val = "val",
  changed_cols_colname = PFUPipelineTools::dataset_info$changed_cols_colname,
  what_to_do_colname = PFUPipelineTools::dataset_info$what_to_do,
  replace_valid_to_version_in_remote =
    PFUPipelineTools::dataset_info$replace_valid_to_version_in_remote,
  delete_row_in_remote = PFUPipelineTools::dataset_info$delete_row_in_remote,
  replace_value_in_remote = PFUPipelineTools::dataset_info$replace_value_in_remote,
  upload_new_row = PFUPipelineTools::dataset_info$upload_new_row,
  no_action = PFUPipelineTools::dataset_info$no_action,
  current_version_int = PFUPipelineTools::version_info$current_version_int,
  tol = 1e-06
)
```

## Arguments

- remote_df:

  A remote version of the rows contained in `local_df`.

- local_df:

  A new data frame computed locally that contains a new set of values
  for `remote_df`.

- valid_from_version_colname, valid_to_version_colname:

  See
  [dataset_info](https://energyeconomydecoupling.github.io/PFUPipelineTools/reference/dataset_info.md).
  Defaults are
  [dataset_info](https://energyeconomydecoupling.github.io/PFUPipelineTools/reference/dataset_info.md)`$valid_from_version_colname`
  and
  [dataset_info](https://energyeconomydecoupling.github.io/PFUPipelineTools/reference/dataset_info.md)`$valid_to_version_colname`
  or "ValidFromVersion" and "ValidToVersion".

- value_colname:

  The name of the value column. Default is
  [mat_colnames](https://energyeconomydecoupling.github.io/PFUPipelineTools/reference/mat_colnames.md)`$value`
  or "value".

- nam:

  The name of a column of names for values in the remote and local data
  frames. This name is used internally. Default is "nam".

- val:

  The name of a column of values in the remote and local data frames.
  This name is used internally. Default is "val".

- changed_cols_colname:

  The name of a column that tells which columns have changed. This
  column is added internally but removed before returning. Default is
  "ChangedCols".

- what_to_do_colname:

  The name of the column that tells what to do with the row. Default is
  [dataset_info](https://energyeconomydecoupling.github.io/PFUPipelineTools/reference/dataset_info.md)`$what_to_do`
  or "WhatToDo".

- replace_valid_to_version_in_remote:

  The string that indicates the value in the remote's `valid_to_version`
  column should be changed. Default is
  [dataset_info](https://energyeconomydecoupling.github.io/PFUPipelineTools/reference/dataset_info.md)`$replace_valid_to_version_in_remote`
  or "Replace ValidToVersion in remote".

- delete_row_in_remote:

  The string that indicates the row should be deleted from the remote
  database. Default is
  [dataset_info](https://energyeconomydecoupling.github.io/PFUPipelineTools/reference/dataset_info.md)`$delete_row_in_remote`
  or "Delete row in remote".

- replace_value_in_remote:

  The string that indicates the value of remote's `value` column should
  be changed. Default is
  [dataset_info](https://energyeconomydecoupling.github.io/PFUPipelineTools/reference/dataset_info.md)`$replace_value_in_remote`
  or "Replace value in remote".

- upload_new_row:

  The string that indicates local rows have new metadata and need to be
  uploaded to the remote. Default is
  [dataset_info](https://energyeconomydecoupling.github.io/PFUPipelineTools/reference/dataset_info.md)`$upload_new`
  or "Upload new row".

- no_action:

  The string that indicates no action is needed on this row. Default is
  [dataset_info](https://energyeconomydecoupling.github.io/PFUPipelineTools/reference/dataset_info.md)`no_action`
  or "No action".

- current_version_int:

  The integer representing the current version. Default is
  [version_info](https://energyeconomydecoupling.github.io/PFUPipelineTools/reference/version_info.md)`$current_version_int`
  or 2147483647.

- tol:

  The tolerance within which a local value will be assumed same as the
  remote value. Default is `1e-6`.

## Value

A data frame with same columns as `remote_df` and `local_df` and an
added column (`what_to_do_colname`).

## Details

In the context of this function, `remote` means (sometimes older) data
from the remote database. `local` means new data calculated locally and
meant to be uploaded to the remote database.

Very important: `remote_df` is assumed to have the same metadata
(non-value columns, i.e., primary keys, excluding the version columns)
as `local_df`. That assumption is valid when called from
[`do_upsert_and_compress()`](https://energyeconomydecoupling.github.io/PFUPipelineTools/reference/do_upsert_and_compress.md),
which already performs a
[`dplyr::semi_join()`](https://dplyr.tidyverse.org/reference/filter-joins.html)
to filter `remote_df` for matching metadata columns.

Important: `local_df` is assumed to contain a complete set of current
information for metadata columns (columns excluding `i`, `j`, and
`value`). Thus, if for the same metadata and the current version,
`local_df` is lacking some rows present in `remote_df`, those rows will
be removed from `remote_df`.

This function doesn't change any rows in the remote database. Rather, it
returns a data frame with same columns as `remote_df` and an additional
column named with the value of `what_to_do_colname` (by default
[dataset_info](https://energyeconomydecoupling.github.io/PFUPipelineTools/reference/dataset_info.md)`$what_to_do`
or "WhatToDo") that tells what must be done with each row and modify the
remote database appropriately. The possible values of the
`what_to_do_colname` are

- [dataset_info](https://energyeconomydecoupling.github.io/PFUPipelineTools/reference/dataset_info.md)`$replace_valid_to_version_in_remote`
  or "Replace ValidToVersion in remote",

- [dataset_info](https://energyeconomydecoupling.github.io/PFUPipelineTools/reference/dataset_info.md)`$replace_value_in_remote`
  or "Replace value in remote",

- [dataset_info](https://energyeconomydecoupling.github.io/PFUPipelineTools/reference/dataset_info.md)`$delete_row_in_remote`
  or "Delete row in remote", and

- [dataset_info](https://energyeconomydecoupling.github.io/PFUPipelineTools/reference/dataset_info.md)`$upload_new_row`
  or "Upload new row".

Respectively, these indicate whether to change the remote table's
`ValidToVersion` value, replace the value in the `value` column, delete
the remote row, or upload a new row, respectively.

Functions that call `compress_helper()` should query the value of
`what_to_do_colname` to decide how to handle each row.

If both `remote_df` and `local_df` are `NULL`, `NULL` is returned. If
only `remote_df` is `NULL`, all rows in `local_df` are assumed to
require uploading to the remote. If only `local_df` is `NULL`, no rows
need to be uploaded to the remote and no rows in the remote need to be
changed.

Note that both `remote_df` and `local_df` should be encoded data frames,
i.e. their foreign key columns should all be ID integers.

Note: `local_df` should have the same values in both
`valid_from_version_colname` and `valid_to_version_colname` to clearly
indicate the intent of the caller. If not, an error is thrown.

The `valid_to_version_colname` in `remote_df` must contain
`current_version_int`. If not, an error is thrown.

`value_colname` can be vector of length greater than 1, indicating
multiple value columns. If any value in a row of `local_df` is different
from `remote_df`, the entire row is marked as updated.

## Examples

``` r
remote_df <- tibble::tribble(
  ~Dataset, ~ValidFromVersion, ~ValidToVersion, ~Country, ~Year, ~matname, ~i, ~j, ~value,
   5, 2, PFUPipelineTools::version_info$current_version_int, 49, 1971, 2, 1, 1, 11,
   5, 2, PFUPipelineTools::version_info$current_version_int, 49, 1971, 2, 1, 2, 12,
   5, 2, PFUPipelineTools::version_info$current_version_int, 49, 1971, 2, 1, 3, 13,
   5, 2, PFUPipelineTools::version_info$current_version_int, 49, 1971, 2, 2, 1, 21,
   # matname = 3 is U
   5, 2, PFUPipelineTools::version_info$current_version_int, 49, 1971, 3, 1, 1, 110,
   5, 2, PFUPipelineTools::version_info$current_version_int, 49, 1971, 3, 2, 2, 220,
   5, 2, PFUPipelineTools::version_info$current_version_int, 49, 1971, 3, 3, 2, 320,
   # Country = 146 is USA
   # matname = 7 is V
   5, 2, PFUPipelineTools::version_info$current_version_int, 146, 1972, 7, 1, 1, 1100,
   5, 2, PFUPipelineTools::version_info$current_version_int, 146, 1972, 7, 2, 2, 2200,
   5, 2, PFUPipelineTools::version_info$current_version_int, 146, 1972, 7, 3, 5, 3500,
   # matname = 8 is Y
   5, 2, PFUPipelineTools::version_info$current_version_int, 146, 1972, 8, 1, 1, 11000,
   5, 2, PFUPipelineTools::version_info$current_version_int, 146, 1972, 8, 2, 1, 21000,
   5, 2, PFUPipelineTools::version_info$current_version_int, 146, 1972, 8, 1, 2, 12000
)
# Change 2 rows
local_df <- remote_df |>
  dplyr::mutate(
    # Change the version information
    "{PFUPipelineTools::dataset_info$valid_from_version_colname}" := 3,
    "{PFUPipelineTools::dataset_info$valid_to_version_colname}" := 3,
  )
local_df[3, PFUPipelineTools::mat_colnames$value] <- -13
local_df[12, PFUPipelineTools::mat_colnames$value] <- -21000
# Returns only the rows that need to be changed
# and what must be done.
compress_helper(remote_df = remote_df, local_df = local_df)
#> # A tibble: 4 × 10
#>   Dataset ValidFromVersion ValidToVersion Country  Year matname     i     j
#>     <dbl>            <dbl>          <dbl>   <dbl> <dbl>   <dbl> <dbl> <dbl>
#> 1       5                2              2      49  1971       2     1     3
#> 2       5                2              2     146  1972       8     2     1
#> 3       5                3     2147483647      49  1971       2     1     3
#> 4       5                3     2147483647     146  1972       8     2     1
#> # ℹ 2 more variables: value <dbl>, WhatToDo <chr>
```
