# Filter a table from the database using natural expressions

Often when collecting data from the database, filtering is desired. But
filtering based on foreign keys (fks, as stored in the database) is
effectively impossible, because of foreign key encoding to integers.
This function filters based on fk values (typically strings), not fk
keys (typically integers), thereby simplifying the filtering process,
with optional downloading thereafter. By default (`collect = FALSE`), a
`tbl` is returned (and data are not downloaded from the database). Use
[`dplyr::collect()`](https://dplyr.tidyverse.org/reference/compute.html)
later to execute the resulting SQL query and obtain an in-memory data
frame. Or, set `collect = TRUE` to execute the SQL and return an
in-memory data frame.

## Usage

``` r
pl_filter_collect(
  db_table_name,
  ...,
  version_string = PFUPipelineTools::version_info$current_version_string,
  collect = FALSE,
  decode_foreign_keys = TRUE,
  create_matsindf = collect,
  conn,
  schema = schema_from_conn(conn = conn),
  fk_parent_tables = get_all_fk_tables(conn = conn, schema = schema, collect = TRUE),
  index_map_name = "Index",
  index_map = fk_parent_tables[[index_map_name]],
  rctype_table_name = "matnameRCType",
  rctypes = decode_fks(db_table_name = rctype_table_name, collect = TRUE, conn = conn,
    schema = schema, fk_parent_tables = fk_parent_tables),
  matrix_class = c("Matrix", "matrix"),
  matname = PFUPipelineTools::mat_meta_cols$matname,
  matval = PFUPipelineTools::mat_meta_cols$matval,
  rowtype_colname = PFUPipelineTools::mat_meta_cols$rowtype,
  coltype_colname = PFUPipelineTools::mat_meta_cols$coltype,
  valid_from_version_colname = PFUPipelineTools::dataset_info$valid_from_version_colname,
  valid_to_version_colname = PFUPipelineTools::dataset_info$valid_to_version_colname
)
```

## Arguments

- db_table_name:

  The string name of the database table to be filtered.

- ...:

  Filter conditions on the data frame, such as `Country == "USA"` or
  `Year %in% 1960:2020`. These conditions reduce data volume, because
  they are applied prior to downloading from `conn`. If no rows match
  these conditions, a data frame with no rows is returned.

- version_string:

  A string of length `1` or more that indicates the desired version(s).
  Default is `PFUPipelineTools::version_info$current_version_string` or
  "current". `NULL` means to download all versions available in
  `db_table_name`. [`c()`](https://rdrr.io/r/base/c.html) (an empty
  string) returns a zero-row table. If `version_string` is invalid, an
  error will be emitted.

- collect:

  A boolean that tells whether to download the result. Default is
  `FALSE`. See details.

- decode_foreign_keys:

  A boolean that tells whether to decode foreign keys in the result.
  Must be `TRUE` if `...` contains filtering expressions with decoded,
  human-readable values (instead of encoded database integers). Default
  is `TRUE`.

- create_matsindf:

  A boolean that tells whether to create a matsindf data frame from the
  collected data frame. Default is the value of `collect`, such that
  setting `collect = TRUE` also implies `create_matsindf`.

- conn:

  The database connection.

- schema:

  The data model (`dm` object) for the database in `conn`. Default is
  `schema_from_conn(conn = conn)`. See details.

- fk_parent_tables:

  A named list of all parent tables for the foreign keys in
  `db_table_name`. Default is
  `get_all_fk_tables(conn = conn, schema = schema)`. See details.

- index_map_name:

  The name of the table that serves as the index for row and column
  names. Default is "Index".

- index_map:

  The index map for the matrices in the database at `conn`. Default is
  `fk_parent_tables[[index_table_name]]`.

- rctype_table_name:

  The name of the table that contains row and column types. Default is
  "matnameRCType".

- rctypes:

  The table of row and column types for the database at `conn`. Default
  is `fk_parent_tables[[rctype_table_name]]`.

- matrix_class:

  One of "Matrix" (the default) for sparse matrices or "matrix" (the
  base matrix representation in `R`) for non-sparse matrices.

- matname:

  The name of the matrix name column. Default is
  `PFUPipelineTools::mat_meta_cols$matname`.

- matval:

  The name of the matrix value column. Default is
  `PFUPipelineTools::mat_meta_cols$matval`.

- rowtype_colname, coltype_colname:

  The names for row and column type columns in data frames. Defaults are
  `PFUPipelineTools::mat_meta_cols$rowtype` and
  `PFUPipelineTools::mat_meta_cols$coltype`, respectively.

- valid_from_version_colname, valid_to_version_colname:

  Names for columns containing version information. Defaults are
  `PFUPipelineTools::dataset_info$valid_from_version_colname` and
  `PFUPipelineTools::dataset_info$valid_to_version_colname`,
  respectively.

## Value

When `collect == TRUE`, a filtered version of `db_table_name` downloaded
from `conn`. When `collect == FALSE`, a list of `tbl` objects.

## Details

Most filter conditions are provide in the `...` argument. By default
(`decode_foreign_keys = TRUE`), decoded values need to be supplied,
e.g., `Country == "AUS"`. However, when `decode_foreign_keys = FALSE`,
you need to supply the encoded integers: `Country == 6`.

Filtering on database versions is a special case because of the way data
are compressed in the database. Specify one or more version values (such
as `c("v1.0", "v1.1", "v2.0")`) in the `version_string` argument returns
all requested versions. The default ("current") returns the current
version matching all filtering criteria in `...`. Set
`version_string = NULL` to disable filtering based on versions, but the
results may be nonsensical.

`schema` is a data model (`dm` object) for the CL-PFU database. It can
be obtained from calling
[`schema_from_conn()`](https://energyeconomydecoupling.github.io/PFUPipelineTools/reference/schema_from_conn.md).
If minimal interaction with the database is desired, be sure to override
the default value for `schema` by supplying a pre-computed `dm` object.

`fk_parent_tables` is a named list of tables, some of which are fk
parent tables containing the mapping between fk values (usually strings)
and fk keys (usually integers) for `db_table_name`. `fk_parent_tables`
is treated as a store from which foreign key tables are retrieved by
name when needed. An appropriate value for `fk_parent_tables` can be
obtained from
[`get_all_fk_tables()`](https://energyeconomydecoupling.github.io/PFUPipelineTools/reference/get_all_fk_tables.md).
If minimal interaction with the database is desired, be sure to override
the default value for `fk_parent_tables` by supplying a pre-computed
named list of foreign key tables.
