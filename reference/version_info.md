# Information for database versions

When storing updated data in the database, we need to identify the
current version. To make this easier, we use a big integer and the
string "current".

## Usage

``` r
version_info
```

## Format

An integer vector with 2 entry.

- current_version_string:

  The string that represents the current version.

- current_version_int:

  The integer that represents the current version.

## Examples

``` r
version_info
#> $current_version_string
#> [1] "current"
#> 
#> $current_version_int
#> [1] 2147483647
#> 
```
