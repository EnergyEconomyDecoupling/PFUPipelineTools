# Copy a country, year subset from one table to another

Inboard copying of tables is much faster than round-tripping data from
the database to the local machine and back. This function does an
inboard filter and copy from `source` to `dest` in `conn`, optionally
emptying `dest` first.

## Usage

``` r
inboard_filter_copy(
  source,
  dest,
  countries = NULL,
  years = NULL,
  empty_dest = TRUE,
  in_place = FALSE,
  dependencies = NULL,
  additional_hash_group_cols = NULL,
  usual_hash_group_cols = PFUPipelineTools::usual_hash_group_cols,
  conn,
  schema = schema_from_conn(conn),
  fk_parent_tables = get_all_fk_tables(conn = conn, schema = schema),
  country = IEATools::iea_cols$country,
  year = IEATools::iea_cols$year,
  pk_suffix = PFUPipelineTools::key_col_info$pk_suffix
)
```

## Arguments

- source:

  A string identifying the source table.

- dest:

  A string identifying the destination table.

- countries:

  Countries to keep.

- years:

  Years to keep.

- empty_dest:

  A boolean that tells whether to empty the destination table before
  copying. Default is `TRUE`.

- in_place:

  A boolean that tells whether to make the changes in the remote
  database at `conn`.

- dependencies:

  Other objects (often targets) upon which the inboard copy depends. The
  default is `NULL`. See details.

- additional_hash_group_cols:

  A vector of strings that gives names of additional columns that should
  *not* be hashed.

- usual_hash_group_cols:

  A string vector that gives typical names of columns that should *not*
  be hashed. Default is
  [`PFUPipelineTools::usual_hash_group_cols`](https://energyeconomydecoupling.github.io/PFUPipelineTools/reference/usual_hash_group_cols.md).

- conn:

  A database connection.

- schema:

  The data model (`dm` object) for the database in `conn`. Default is
  `dm_from_conn(conn)`. See details.

- fk_parent_tables:

  A named list of all parent tables for the foreign keys in
  `db_table_name`. See details.

- country:

  The name of the country column in `source` and `dest`. Default is
  `IEATools::iea_cols$country`.

- year:

  The name of the year column in `source` and `dest`. Default is
  `IEATools::iea_cols$year`.

- pk_suffix:

  The suffix for primary key columns. Default is
  `PFUPipelineTools::key_col_info$pk_suffix`.

## Value

A hash of the destination data frame created by
[`pl_upsert()`](https://energyeconomydecoupling.github.io/PFUPipelineTools/reference/pl_upsert.md).

## Details

The `source` and `dest` tables should have identical columns.

The `dependencies` argument can be a vector of other objects upon which
the desired inboard copy depends. Typically, the target that makes
`source` should be given in `dependencies`, for unless the target that
makes `source` completes, the inboard filter copy will fail.
`dependencies` is ignored internally.

`schema` is a data model (`dm` object) for the database in `conn`. Its
default value (`schema_from_conn(conn)`) extracts the data model for the
database at `conn` automatically. However, if the caller already has the
data model, supplying it in the `schema` argument will save time.

`fk_parent_tables` is a named list of tables, one of which (the one
named `db_table_name`) contains the foreign keys for `db_table_name`.
`fk_parent_tables` is treated as a store from which foreign key tables
are retrieved by name when needed. The default value (which calls
[`get_all_fk_tables()`](https://energyeconomydecoupling.github.io/PFUPipelineTools/reference/get_all_fk_tables.md)
with `collect = TRUE` because decoding of foreign keys is done outboard
of the database) retrieves all possible foreign key parent tables from
`conn`, potentially a time-consuming process. For speed, pre-compute all
foreign key parent tables once (via `get_all_fk_tables(collect = TRUE)`)
and pass the list to the `fk_parent_tables` argument of this function.
