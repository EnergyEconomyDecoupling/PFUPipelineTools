# Metadata information for aggregation groups

A string list containing metadata information for aggregation groups.

## Usage

``` r
agg_metadata
```

## Format

A string list with 6 entries.

- total_value:

  A total value.

- all_value:

  An aggregation of all items.

- product_value:

  An aggregation of products.

- sector_value:

  An aggregation across sectors.

- flow_value:

  An aggregation across flows.

- none:

  No aggregation.

## Examples

``` r
agg_metadata
#> $total_value
#> [1] "Total"
#> 
#> $all_value
#> [1] "All"
#> 
#> $product_value
#> [1] "Product"
#> 
#> $sector_value
#> [1] "Sector"
#> 
#> $flow_value
#> [1] "Flow"
#> 
#> $none
#> [1] "None"
#> 
```
