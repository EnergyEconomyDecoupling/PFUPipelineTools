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

