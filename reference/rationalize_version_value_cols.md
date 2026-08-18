# Adjust column titles while deciding next steps

When deciding what to do with new or adjusted data, this function
adjusts title of joined version and value columns. This function is used
as a helper function for
[`prep_out()`](https://energyeconomydecoupling.github.io/PFUPipelineTools/reference/prep_out.md).

## Usage

``` r
rationalize_version_value_cols(
  .df,
  version_suffix_to_remove,
  value_suffix_to_remove,
  valid_from_version_colname,
  valid_to_version_colname,
  value_colname,
  remote_suff,
  local_suff
)
```

## Arguments

- .df:

  The data frame whose column names are to be adjusted.

- version_suffix_to_remove:

  The string suffix on version column names that are to be deleted.

- value_suffix_to_remove:

  The string suffix on value column names that are to be deleted.

- valid_from_version_colname:

  The string name of the ValidFromVersion column.

- valid_to_version_colname:

  The string name of the ValidToVersion column.

- value_colname:

  The string name of the value columns. May be a vector of multiple
  value columns.

- remote_suff:

  The remote suffix for column names.

- local_suff:

  The local suffix for column names.

## Value

A modified version of `.df` that is amenable to uploading or adjusting a
remote data frame.
