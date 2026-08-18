# Aggregation file tab information

A string list containing the aggregation file's tab names.

## Usage

``` r
aggregation_file_tab_names
```

## Format

A string list with 6 entries.

- region_aggregation:

  The name of the region aggregation tab, "region_aggregation".

- continent_aggregation:

  The name of the continent aggregation tab, "continent_aggregation".

- world_aggregation:

  The string name of the world aggregation tab. "world_aggregation".

- ef_product_aggregation:

  The string name of the final energy product aggregation tab.
  "ef_product_aggregation".

- eu_product_aggregation:

  The string name of the useful energy product aggregation tab.
  "eu_product_aggregation".

- ef_sector_aggregation:

  The string name of the final energy sector aggregation tab.
  "ef_sector_aggregation".

## Examples

``` r
aggregation_file_tab_names
#> $region_aggregation
#> [1] "region_aggregation"
#> 
#> $continent_aggregation
#> [1] "continent_aggregation"
#> 
#> $world_aggregation
#> [1] "world_aggregation"
#> 
#> $ef_product_aggregation
#> [1] "ef_product_aggregation"
#> 
#> $eu_product_aggregation
#> [1] "eu_product_aggregation"
#> 
#> $ef_sector_aggregation
#> [1] "ef_sector_aggregation"
#> 
```
