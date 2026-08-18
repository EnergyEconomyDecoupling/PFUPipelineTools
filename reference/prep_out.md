# Prepare an outgoing data frame for deciding what to do with new data

This is a helper function for
[`compress_helper()`](https://energyeconomydecoupling.github.io/PFUPipelineTools/reference/compress_helper.md).

## Usage

``` r
prep_out(
  next_steps_df,
  local_version,
  previous_version,
  valid_from_version_colname,
  valid_to_version_colname,
  value_colname,
  remote_suff,
  local_suff,
  out_template,
  what_to_do_colname,
  replace_valid_to_version_in_remote,
  delete_row_in_remote,
  replace_value_in_remote,
  upload_new_row,
  no_action,
  current_version_int
)
```

## Arguments

- next_steps_df:

  A data frame from
  [`compress_helper()`](https://energyeconomydecoupling.github.io/PFUPipelineTools/reference/compress_helper.md).

- local_version:

  An integer that tells the local version being developed.

- previous_version:

  An integer that tells the previous version before the version on which
  we're working.

- valid_from_version_colname:

  The name of the ValidFromVersion column.

- valid_to_version_colname:

  The name of the ValidToVersion column.

- value_colname:

  The names of the value columns. May be a vector of length greater than
  1.

- remote_suff:

  The suffix for remote column names.

- local_suff:

  The suffix for local column names.

- out_template:

  A zero-row template data frame for the outgoing data frame.

- what_to_do_colname:

  The name of a column that tells what to do with each row.

- replace_valid_to_version_in_remote:

  A string that indicates a row should have its ValidToVersion value
  replaced in the remote table.

- delete_row_in_remote:

  A string that indicates a row should be deleted from the remote table.

- replace_value_in_remote:

  A string that indicates values should be replaced in the remote table.

- upload_new_row:

  A string that indicates the row is new and should be uploaded to the
  remote table.

- no_action:

  A string that indicates there should be no action taken upon this row.

- current_version_int:

  The integer that indicates a row is the current version.

## Value

A modified version of `next_steps_df` indicating what should be done for
each row.
