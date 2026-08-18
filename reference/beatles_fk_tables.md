# Example simple database tables

A named list of data frames, each of which is a foreign key table (fk
table) with simple information about the Beatles.

## Usage

``` r
beatles_fk_tables
```

## Format

A named list of data frames.

## Details

This list is in the correct format for the function
[`pl_upload_schema_and_simple_tables()`](https://energyeconomydecoupling.github.io/PFUPipelineTools/reference/pl_upload_schema_and_simple_tables.md).

## Examples

``` r
beatles_fk_tables
#> $Member
#> # A tibble: 4 × 2
#>   MemberID Member         
#>      <int> <chr>          
#> 1        1 John Lennon    
#> 2        2 Paul McCartney 
#> 3        3 George Harrison
#> 4        4 Ringo Starr    
#> 
#> $Role
#> # A tibble: 4 × 2
#>   RoleID Role       
#>    <int> <chr>      
#> 1      1 Lead singer
#> 2      2 Bassist    
#> 3      3 Guitarist  
#> 4      4 Drummer    
#> 
```
