# Exemplar table names

A string list containing named names of columns and tabs for exemplar
tables. Items in the list provide default values for column name
function arguments throughout the `PFUPipeline` package.

## Usage

``` r
exemplar_names
```

## Format

A string list with 16 entries.

- exemplar_tab_name:

  The string name of the tab in the Excel file containing the exemplar
  table.

- prev_names:

  The name of a column of previous names used for the country.

- exemplars:

  The name of a column of exemplar countries.

- exemplar_country:

  The name of an exemplar country column.

- exemplar_countries:

  The name of an exemplar countries column.

- exemplar_tables:

  The name of a column containing exemplar tables.

- iea_data:

  The name of a column containing IEA extended energy balance data.

- alloc_data:

  The name of a column containing final-to-useful allocation data.

- incomplete_alloc_table:

  The name of a column containing incomplete final-to-useful allocation
  tables.

- complete_alloc_table:

  The name of a column containing completed final-to-useful allocation
  tables.

- incomplete_eta_table:

  The name of a column containing incomplete final-to-useful efficiency
  tables.

- complete_eta_table:

  The name of a column containing completed final-to-useful efficiency
  tables.

- region_code:

  The name of the region code column.

- country_name:

  The name of the column containing the long name of a country.

- agg_code_col:

  The metadata column "Agg.Code", representing the country, or country
  group code for individual country level data to be aggregated in to.

- world:

  The name of the world region.

## Examples

``` r
exemplar_names
#> $exemplar_tab_name
#> [1] "exemplar_table"
#> 
#> $prev_names
#> [1] "PrevNames"
#> 
#> $exemplars
#> [1] "Exemplars"
#> 
#> $exemplar_country
#> [1] "ExemplarCountry"
#> 
#> $exemplar_countries
#> [1] "ExemplarCountries"
#> 
#> $exemplar_tables
#> [1] "ExemplarTables"
#> 
#> $iea_data
#> [1] "IEAData"
#> 
#> $alloc_data
#> [1] "AllocData"
#> 
#> $incomplete_alloc_table
#> [1] "IncompleteAllocTable"
#> 
#> $complete_alloc_table
#> [1] "CompleteAllocTable"
#> 
#> $incomplete_eta_table
#> [1] "IncompleteEtaTable"
#> 
#> $complete_eta_table
#> [1] "CompleteEtaTable"
#> 
#> $region_code
#> [1] "RegionCode"
#> 
#> $country_name
#> [1] "CountryName"
#> 
#> $agg_code_col
#> [1] "AggCode"
#> 
#> $world
#> [1] "WRLD"
#> 
```
