# Extract specific country and year data

Data is extracted according to the `countries` and `years` objects in a
way that is amenable to drake subtargets. `dplyr:` `:filter()` does the
subsetting.

## Usage

``` r
filter_countries_years(
  .df,
  countries,
  years,
  country = IEATools::iea_cols$country,
  year = IEATools::iea_cols$year
)
```

## Arguments

- .df:

  A data frame containing cleaned data with lots of countries and years.

- countries:

  A list of country codes for countries to be analyzed. "all" means
  return all countries.

- years:

  A vector of years. "all" means return all years.

- country, year:

  See
  [`IEATools::iea_cols`](https://matthewheun.github.io/IEATools/reference/iea_cols.html).

## Value

A data frame with the desired IEA data only.

## Examples

``` r
IEATools::sample_iea_data_path() |>
  IEATools::load_tidy_iea_df() |>
  filter_countries_years(countries = c("ZAF"), years = 1960:1999)
#> # A tibble: 98 × 11
#>    Country Method EnergyType LastStage  Year LedgerSide FlowAggregationPoint    
#>    <chr>   <chr>  <chr>      <chr>     <dbl> <chr>      <chr>                   
#>  1 ZAF     PCM    E          Final      1971 Supply     Total primary energy su…
#>  2 ZAF     PCM    E          Final      1971 Supply     Total primary energy su…
#>  3 ZAF     PCM    E          Final      1971 Supply     Total primary energy su…
#>  4 ZAF     PCM    E          Final      1971 Supply     Total primary energy su…
#>  5 ZAF     PCM    E          Final      1971 Supply     Total primary energy su…
#>  6 ZAF     PCM    E          Final      1971 Supply     Total primary energy su…
#>  7 ZAF     PCM    E          Final      1971 Supply     Total primary energy su…
#>  8 ZAF     PCM    E          Final      1971 Supply     Total primary energy su…
#>  9 ZAF     PCM    E          Final      1971 Supply     Total primary energy su…
#> 10 ZAF     PCM    E          Final      1971 Supply     Total primary energy su…
#> # ℹ 88 more rows
#> # ℹ 4 more variables: Flow <chr>, Product <chr>, Unit <chr>, Edot <dbl>
```
