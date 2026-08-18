# Sources for phi values

A string list containing named sources of phi (exergy-to-energy ratio)
values.

## Usage

``` r
phi_sources
```

## Format

A string list with 3 entries.

- eta_fu_tables:

  Tables of final-to-useful efficiency values.

- temperature_data:

  Country-average yearly temperature data.

- phi_constants:

  Tables of constant phi values.

## Examples

``` r
phi_sources
#> $eta_fu_tables
#> [1] "etafuTables"
#> 
#> $temperature_data
#> [1] "TemperatureData"
#> 
#> $phi_constants
#> [1] "phiConstants"
#> 
```
