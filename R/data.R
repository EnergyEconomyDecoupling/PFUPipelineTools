#' Country abbreviations
#'
#' A string list containing all known countries in abbreviated format.
#' 3-letter codes are analyzed as separate countries.
#' The 3-letter codes conform to ISO naming conventions.
#' 4-letter codes are not ISO codes but still analyzed as separate countries.
#' 5-letter codes (and longer) are aggregations.
#'
#' @format A string list with `r length(all_countries)` entries.
#'
#' @examples
#' all_countries
"all_countries"


#' Double-counted countries
#'
#' A string list containing abbreviated names of double-counted countries.
#'
#' @format A string list with `r length(double_counted_countries)` entries.
#'
#' @examples
#' double_counted_countries
"double_counted_countries"


#' Canonical countries
#'
#' A string list containing abbreviated names of canonical countries.
#' Canonical countries is the set difference between
#' `all_countries` and `double-counted_countries`.
#'
#' @format A string list with `r length(canonical_countries)` entries.
#'
#' @examples
#' canonical_countries
"canonical_countries"


#' Column names in schema tables
#'
#' A string list names of columns in schema tables.
#'
#' @format A string list with `r length(schema_table_colnames)` entries.
#' \describe{
#' \item{table}{The name of a string column that tells the database table name.}
#' \item{colname}{The name of a string column that identifies a column in `table`.}
#' \item{is_pk}{The name of a boolean column that tells whether `column` is a primary key.}
#' \item{coldatatype}{The name of a string column that tells the data type of `column`, such as "int", "text", "boolean", or "double precision".}
#' \item{fk_table}{The name of a string column that identifies the table in which a foreign key can be found. "NA" is a valid value for `fk_table`, meaning that `colname` does not have a foreign key.}
#' \item{fk_colname}{The name of a string column that identifies the name of a column in `fk_table` that contains values for the foreign key. "NA" is a valid value for `fk_colname`, meaning that `colname` does not have a foreign key.}
#' }
#'
#' @examples
#' schema_table_colnames
"schema_table_colnames"


#' Column names in primary key tables from `dm`
#'
#' `dm::dm_get_all_pks()` returns a data frame with several columns.
#' This constant gives those names as a string list
#' to document their meaning and
#' to guard against future changes.
#'
#' @format A string list with `r length(dm_pk_colnames)` entries.
#' \describe{
#' \item{table}{The name of a string column that tells table names.}
#' \item{pk_col}{The name of a string column that identifies primary key columns in `table`.}
#' \item{autoincrement}{The name of a boolean column that tells whether `pk_col` is auto-incrementing.}
#' }
#'
#' @examples
#' dm_pk_colnames
"dm_pk_colnames"


#' Column names in foreign key tables from `dm`
#'
#' `dm::dm_get_all_fks()` returns a data frame with several columns.
#' This constant gives those names as a string list
#' to document their meaning and
#' to guard against future changes.
#'
#' In this context, a "parent" table contains the foreign key definition
#' and its values.
#' A "child" table contains at least one foreign column that references
#' a column in the parent table.
#'
#' @format A string list with `r length(dm_fk_colnames)` entries.
#' \describe{
#' \item{child_table}{The name of a string column that tells child table names.}
#' \item{child_fk_cols}{The name of a string column that identifies foreign key columns in the child table.}
#' \item{parent_table}{The name of a string column that identifies the table where the child's foreign key is defined.}
#' \item{parent_key_cols}{The name of a string column that tells which column in the parent table contains the definition of the foreign key.}
#' \item{on_delete}{Tells what will happen when the foreign key column is deleted.}
#' }
#'
#' @examples
#' dm_fk_colnames
"dm_fk_colnames"


#' Key column info
#'
#' A string list containing information about database table keys.
#'
#' @format A string list with `r length(key_col_info)` entries.
#'
#' @examples
#' key_col_info
"key_col_info"


#' Example database schema table
#'
#' A data frame containing several columns
#' that describe a database schema
#' for simple facts about the Beatles.
#'
#' @format A data frame with columns "Table", "colname", "coldatatype", "fk.table", and "fk.colname".
#'
#' @examples
#' beatles_schema_table
"beatles_schema_table"


#' Example simple database tables
#'
#' A named list of data frames, each of which
#' is a foreign key table (fk table)
#' with simple information about the Beatles.
#'
#' This list is in the correct format
#' for the function `pl_upload_schema_and_simple_tables()`.
#'
#' @format A named list of data frames.
#'
#' @examples
#' beatles_fk_tables
"beatles_fk_tables"

