# Example database schema table

A data frame containing several columns that describe a database schema
for simple facts about the Beatles.

## Usage

``` r
beatles_schema_table
```

## Format

A data frame with columns "Table", "colname", "coldatatype", "fk.table",
and "fk.colname".

## Examples

``` r
beatles_schema_table
#> # A tibble: 6 × 6
#>   TableName  Colname  IsPK  ColDataType FKTable FKColname
#>   <chr>      <chr>    <lgl> <chr>       <chr>   <chr>    
#> 1 Member     MemberID TRUE  int         NA      NA       
#> 2 Member     Member   FALSE text        NA      NA       
#> 3 Role       RoleID   TRUE  int         NA      NA       
#> 4 Role       Role     FALSE text        NA      NA       
#> 5 MemberRole Member   TRUE  int         Member  MemberID 
#> 6 MemberRole Role     FALSE int         Role    RoleID   
```
