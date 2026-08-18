# Usual columns to be preserved when hashing target uploads

A string list containing names of the usual columns to preserve on
upload, in addition to columns that contain only one unique value.

## Usage

``` r
usual_hash_group_cols
```

## Format

A string list with 9 entries.

- dataset:

  The string name of the dataset column.

- table_name:

  The string name of the table name column.

- country:

  The string name of the country column.

- method:

  The string name of the method column.

- year:

  The string name of the year column.

- last_stage:

  The string name of the last_stage column.

- energy_type:

  The string name of the energy_type column.

- tar_group:

  The string name of the tar_group column.

## Examples

``` r
usual_hash_group_cols
#>       dataset    table_name       country        method          year 
#>     "Dataset" "DBTableName"     "Country"      "Method"        "Year" 
#>    last_stage   energy_type  includes_neu     tar_group 
#>   "LastStage"  "EnergyType" "IncludesNEU"   "tar_group" 
```
