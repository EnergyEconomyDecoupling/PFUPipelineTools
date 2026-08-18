# PFUAggPipeline data frame column names

Aggregation data frame column names

## Usage

``` r
aggregation_df_cols
```

## Format

A string list with 9 entries.

- product_aggregation:

  The name of the metadata column that tells about product aggregation.
  "ProductAggregation"

- industry_aggregation:

  The name of the metadata column that tells about industry aggregation.
  "IndustryAggregation"

- specified:

  The value that indicates products or industries remain is specified.
  "Specified"

- despecified:

  The value that indicates products or industries have been despecified
  and aggregated. "Despecified"

- ungrouped:

  The value that indicates products or industries have not been grouped.
  "Ungrouped"

- grouped:

  The value that indicates products or industries have been grouped.
  "Grouped"

- chopped_mat:

  The value that indicates which matrix has been chopped. "ChoppedMat"

- chopped_var:

  The value that indicates the chopping product or industry. "ChopVar"

- product_sector:

  The column containing values for chopped_var.
  `Recca::aggregate_cols$product_sector`.

## Details

A string list containing names of column names and values for
aggregation data frames.

## Examples

``` r
aggregation_df_cols
#> $product_aggregation
#> [1] "ProductAggregation"
#> 
#> $industry_aggregation
#> [1] "IndustryAggregation"
#> 
#> $specified
#> [1] "Specified"
#> 
#> $despecified
#> [1] "Despecified"
#> 
#> $ungrouped
#> [1] "Ungrouped"
#> 
#> $grouped
#> [1] "Grouped"
#> 
#> $chopped_mat
#> [1] "ChoppedMat"
#> 
#> $chopped_var
#> [1] "ChoppedVar"
#> 
#> $product_sector
#> [1] "ProductIndustrySector"
#> 
```
