# Column name for datasets

A string list containing the column name for datasets.

## Usage

``` r
dataset_info
```

## Format

A string list with 22 entries.

- dataset_colname:

  The string name of the dataset column, "Dataset".

- valid_from_version_colname:

  The string name of the column that gives the initial version for which
  this datapoint is valid, "ValidFromVersion".

- valid_to_version_colname:

  The string name of the column that gives the last version for which
  this datapoint is valid, "ValidToVersion".

- clpfu_iea:

  A string identifying that ECC data are from the IEA exclusively.

- clpfu_mw:

  A string identifying that ECC data are for muscle work (MW)
  exclusively.

- clpfu_iea_mw:

  A string identifying that ECC data include both IEA and muscle work.

- clpfu:

  A string identifying the CL-PFU dataset.

- ieaeweb:

  A string identifying the IEA's Extended World Energy Balance dataset.

- faostat:

  A string identifying the UN's Food and Agriculture Organization
  dataset.

- ilostat:

  A string identifying the UN's International Labour Organization
  dataset.

- wlrpfu:

  A string identifying the World Long Run Primary Final Useful dataset.

- wlrpfu_elect:

  A string identifying the electricity portion of the World Long Run
  Primary Final Useful dataset.

- wlrpfu_trans:

  A string identifying the transport portion of the World Long Run
  Primary Final Useful dataset.

- wlrpfu_mw:

  A string identifying the muscle work portion of the World Long Run
  Primary Final Useful dataset.

- changed_cols_colname:

  A string that identifies which value columns have changed.

- what_to_do:

  The string name of a column that tells what to do with updated data
  for the remote database. Options are `change_remote` and `upload_new`.

- delete_or_change_valid_to_in_remote:

  A string that indicates we need to either delete a row in the remote
  or change the ValidToVersion column in the remote.

- replace_valid_to_version_in_remote:

  A string indicating that valid to version column should be changed in
  the remote database when updating values.

- replace_value_in_remote:

  A string that indicates the value should be replaced in the remote
  database.

- delete_row_in_remote:

  A string that indicates a row to be deleted from the remote database.

- upload_new_row:

  A string indicating rows of a local dataframe that have new metadata
  and should be uploaded to the remote database. They do not yet exist
  in the remote database.

- no_action:

  A string indicating that rows of a dataframe need no action taken upon
  them, because they are unchanged.

## Examples

``` r
dataset_info
#> $dataset_colname
#> [1] "Dataset"
#> 
#> $valid_from_version_colname
#> [1] "ValidFromVersion"
#> 
#> $valid_to_version_colname
#> [1] "ValidToVersion"
#> 
#> $clpfu_iea
#> [1] "CL-PFU IEA"
#> 
#> $clpfu_mw
#> [1] "CL-PFU MW"
#> 
#> $clpfu_iea_mw
#> [1] "CL-PFU IEA+MW"
#> 
#> $clpfu
#> [1] "CL-PFU"
#> 
#> $ieaeweb
#> [1] "IEA EWEB"
#> 
#> $faostat
#> [1] "FAOSTAT"
#> 
#> $ilostat
#> [1] "ILOSTAT"
#> 
#> $wlrpfu
#> [1] "WLR-PFU"
#> 
#> $wlrpfu_elect
#> [1] "WLR-PFU Electricity"
#> 
#> $wlrpfu_trans
#> [1] "WLR-PFU Transport"
#> 
#> $wlrpfu_mw
#> [1] "WLR-PFU Muscle work"
#> 
#> $changed_cols_colname
#> [1] "ChangedCols"
#> 
#> $what_to_do
#> [1] "WhatToDo"
#> 
#> $delete_or_change_valid_to_in_remote
#> [1] "Delete or change ValidToVersion in remote"
#> 
#> $replace_valid_to_version_in_remote
#> [1] "Replace ValidToVersion in remote"
#> 
#> $replace_value_in_remote
#> [1] "Replace value in remote"
#> 
#> $delete_row_in_remote
#> [1] "Delete row in remote"
#> 
#> $upload_new_row
#> [1] "Upload new row"
#> 
#> $no_action
#> [1] "No action"
#> 
```
