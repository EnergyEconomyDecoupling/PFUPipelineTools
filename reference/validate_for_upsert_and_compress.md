# Validate and prepare the incoming data frame for `upsert_and_compress()`.

Validate and prepare the incoming data frame for
`upsert_and_compress()`.

## Usage

``` r
validate_for_upsert_and_compress(
  .df,
  db_table_name,
  valid_from_version_colname,
  valid_to_version_colname,
  version_string,
  current_version_string
)
```

## Arguments

- .df:

  The data frame being validated.

- db_table_name:

  A string identifying the destination for `.df` in `conn`, i.e. the
  name of a remote database table. Default is `NULL`, meaning that the
  value for this argument will be taken from the `.db_table_name` column
  of `.df`.

- valid_from_version_colname:

  The string name of the valid from version column. Default is
  [dataset_info](https://energyeconomydecoupling.github.io/PFUPipelineTools/reference/dataset_info.md)`$valid_from_version_colname`
  or "ValidFromVersion". Cannot be
  `PFUPipelineTools::version_info$current_version_string` or "current".

- valid_to_version_colname:

  The string name of the valid to version column. Default is
  [dataset_info](https://energyeconomydecoupling.github.io/PFUPipelineTools/reference/dataset_info.md)`$valid_to_version_colname`
  or "ValidToVersion". Cannot be
  `PFUPipelineTools::version_info$current_version_string` or "current".

- version_string:

  An optional string that tells the version of the database being
  updated. See details. Default is `NULL`, meaning version information
  should be obtained from the `ValidFromVersion` and `ValidToVersion`
  columns of `.df`.

- current_version_string:

  A string that identifies the current version in the remote table.
  Default is `PFUPipelineTools::version_info$current_version_string` or
  current. It is probably a *very bad* idea to supply a different value
  from the default.

## Value

An updated version of `.df` ready for upserting and compressing.
