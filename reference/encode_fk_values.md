# Encode a vector of foreign key values according to a foreign key parent table

In a database, there are foreign key tables containing fk values
(usually strings) and fk keys (usually integers). This function converts
a vector of fk values (strings, `v_val`) into a vector fk keys
(integers) according to the database at `conn`.

## Usage

``` r
encode_fk_values(
  v_val,
  fk_table_name,
  conn = NULL,
  schema = schema_from_conn(conn),
  fk_parent_tables = get_all_fk_tables(conn = conn, schema = schema, collect = TRUE),
  pk_suffix = PFUPipelineTools::key_col_info$pk_suffix
)
```

## Arguments

- v_val:

  A vector of fk values (typically strings) to be converted to fk keys
  (typically integers).

- fk_table_name:

  The name of the foreign key table in `conn` that contains the mapping
  from fk values to fk keys.

- conn:

  A connection to the CL-PFU database. Needed only if `fk_parent_tables`
  is not provided. Default is `NULL`.

- schema:

  The data model (`dm` object) for the database in `conn`. Default is
  `dm_from_con(conn, learn_keys = TRUE)`. Needed only if
  `fk_parent_tables` is not provided. See details.

- fk_parent_tables:

  A named list of all parent tables for the foreign keys in
  `db_table_name`. See details.

- pk_suffix:

  A string that gives the suffix for primary key columns. Default is
  `PFUPipelineTools::key_col_info$pk_suffix`.

## Value

`v_val` encoded according to `fk_parent_tables`.

## Details

`schema` is a data model (`dm` object) for the database in `conn`. Its
default value (`schema_from_conn(conn)`) extracts the data model for the
database at `conn` automatically. However, if the caller already has the
data model, supplying it in the `schema` argument will save time.

`fk_parent_tables` is a named list of tables, one of which (the one
named `fk_table_name`) contains the foreign keys `v`. `fk_parent_tables`
is treated as a store from which foreign key tables are retrieved by
name when needed. The default value (which calls
[`get_all_fk_tables()`](https://energyeconomydecoupling.github.io/PFUPipelineTools/reference/get_all_fk_tables.md)
with `collect = TRUE`) retrieves all possible foreign key parent tables
from `conn`, potentially a time-consuming process. For speed,
pre-compute all foreign key parent tables once (via
`get_all_fk_tables(collect = TRUE)`) and pass the list to the
`fk_parent_tables` argument of this function.

## Examples

``` r
fk_parent_tables <- list(
  Country = tibble::tribble(~CountryID, ~Country,
                            1, "USA",
                            2, "ZAF",
                            3, "GHA"))
encode_fk_values(c("USA", "USA", "GHA"),
                 fk_table_name = "Country",
                 fk_parent_tables = fk_parent_tables)
#> [1] 1 1 3
```
