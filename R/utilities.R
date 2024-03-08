#' Self-name a list
#'
#' When using the native pipe operator,
#' it is sometimes convenient to self-name
#' a vector or list,
#' especially for later use in `lapply()` and friends.
#'
#' Internally, this function calls
#' `magrittr::set_names(x, x)`.
#'
#' @param x The character vector or list of character strings
#'          to be self-named.
#'
#' @return A self-named version of `x`.
#'
#' @export
#'
#' @examples
#' c("a", "b", "c") |>
#'   self_name()
#' list("A", "B", "C") |>
#'   self_name()
self_name <- function(x) {
  magrittr::set_names(x, x)
}


#' Get a named list of foreign key tables
#'
#' For a database whose tables have many foreign keys
#' that point to primary key tables,
#' it is helpful to have a list of those foreign key tables
#' for inboard and outboard processing purposes,
#' especially when converting strings to integer keys.
#' This function extracts a named list of data frames,
#' where each data frame is a foreign key table in the database.
#' The data frame items in the list are named
#' by the names of the tables in the database at `conn`.
#'
#' By default, the returned list contains `tbl` objects,
#' with references to the actual tables in `conn`.
#' To return the actual data frames, set `collect` to `TRUE`.
#'
#' `schema` is a data model (`dm` object) for the database in `conn`.
#' Its default value (which calls `schema_fom_conn()`)
#' extracts the data model for the database at `conn` automatically.
#' However, if the caller already has the data model,
#' supplying it in the `schema` argument will save some time.
#'
#' @param conn A connection to a database.
#' @param schema The data model (`dm` object) for the database in `conn`.
#'               Default calls `schema_from_conn(conn)`.
#'               See details.
#' @param collect A boolean that tells whether to
#'                download the foreign key tables.
#'                Default is `FALSE` to enable
#'                inboard processing of queries.
#'
#' @return A named list of data frames,
#'         where each data frame
#'         is a table in `conn` that contains
#'         foreign keys and their values.
#'
#' @export
get_all_fk_tables <- function(conn,
                              schema = schema_from_conn(conn),
                              collect = FALSE) {
  schema |>
    dm::dm_get_all_fks() |>
    magrittr::extract2("parent_table") |>
    unique() |>
    self_name() |>
    lapply(FUN = function(this_parent_table) {
      out <- dplyr::tbl(conn, this_parent_table)
      if (collect) {
        out <- out |>
          dplyr::collect()
      }
      return(out)
    })
}


#' Get the database schema (a `dm` object) from a connection
#'
#' It is helpful to know the actual schema
#' in use by a database at a connection.
#' This function provides this information.
#' It is a thin wrapper around
#' `dm_from_con()` from the package `dm`
#' that enables building the documentation website.
#'
#' @param conn A `DBI::DBIConnection`.
#' @param table_names A character vector of the names of the tables to include.
#'                    `NULL` (the default) means return all tables in `conn`.
#' @param learn_keys A boolean that tells whether to include the definition of primary and final keys
#'                   in the return value.
#'                   Default is `TRUE`.
#' @param .names See documentation for `dm_from_con()` in the `dm` package.
#'               Default is `NULL`.
#' @param table_type Gives the type of table to return.
#'                   Default is "BASE TABLE", which means to return persistent tables,
#'                   the normal table type.
#'
#' @return A `dm` object.
#'
#' @export
schema_from_conn <- function(conn = NULL,
                             table_names = NULL,
                             learn_keys = TRUE,
                             .names = NULL,
                             table_type = "BASE TABLE") {
  dm::dm_from_con(conn, table_names = table_names, learn_keys = TRUE)
}


#' Upload a small database of Beatles information
#'
#' Used only for testing.
#'
#' @param conn The connection to a Postgres database.
#'             The user must have write permission.
#'
#' @return A list of tables containing Beatles information
upload_beatles <- function(conn) {
  PFUPipelineTools::beatles_schema_table |>
    schema_dm() |>
    pl_upload_schema_and_simple_tables(simple_tables = PFUPipelineTools::beatles_fk_tables,
                                       conn = conn,
                                       drop_db_tables = c("MemberRole", "Member", "Role"))
}


#' Clean up Beatles tables
#'
#' Used only for testing.
#'
#' @param conn The connection to a Postgres database.
#'             The user must have write permission.
#'
#' @return A list of tables deleted
clean_up_beatles <- function(conn) {

  beatles_tables <- c("MemberRole", "Member", "Role")
  # Only drop tables that exist
  db_tables <- DBI::dbListTables(conn)
  db_tables_in_beatles_tables <- which(db_tables %in% beatles_tables)
  to_drop <- db_tables[db_tables_in_beatles_tables]

  to_drop |>
    purrr::map(function(this_table_name) {
      DBI::dbExecute(conn, paste0('DROP TABLE "', this_table_name, '" CASCADE;'))
    })
  return(to_drop)
}


#' Copy a country, year subset from one table to another
#'
#' Inboard copying of tables is much faster than round-tripping
#' data from the database to the local machine and back.
#' This function does an inboard filter and copy
#' from `source` to `dest` in `conn`,
#' optionally emptying `dest` first.
#'
#' The `source` and `dest` tables should have identical columns.
#'
#' @param source A string identifying the source table.
#' @param dest A string identifying the destination table.
#' @param conn A database connection.
#' @param countries Countries to keep.
#' @param years Years to keep.
#' @param empty_dest A boolean that tells whether to empty the destination table before copying.
#'                   Default is `TRUE`.
#' @param in_place A boolean that tells whether to make the changes in the remote
#'                 database at `conn`.
#' @param country The name of the country column in `source` and `dest`.
#'                Default is `IEATools::iea_cols$country`.
#' @param year The name of the year column in `source` and `dest`.
#'             Default is `IEATools::iea_cols$year`.
#'
#' @return `TRUE` if successful. An error if not.
#'
#' @export
inboard_filter_copy <- function(source,
                                dest,
                                countries,
                                years,
                                empty_dest = TRUE,
                                in_place = FALSE,
                                conn,
                                schema = schema_from_conn(conn),
                                fk_parent_tables = get_all_fk_tables(conn = conn,
                                                                     schema = schema),
                                country = IEATools::iea_cols$country,
                                year = IEATools::iea_cols$year,
                                pk_suffix = PFUPipelineTools::key_col_info$pk_suffix) {

  if (empty_dest & in_place) {
    empty_table <- dplyr::tbl(conn, dest) |>
      dplyr::filter(FALSE) |>
      dplyr::collect()
    # Overwrite the existing table with the empty one
    DBI::dbWriteTable(conn, name = "dest", value = empty_table, overwrite = TRUE)
  }

  countries_string <- paste0(countries, collapse = ", ")
  years_string <- paste0(years, collapse = ", ")

  # # Add the filtered rows from source into dest
  # insert_rows_stmt <- paste0('INSERT INTO "', dest, '" ',
  #                            'SELECT * ',
  #                            'FROM "', source, '" ',
  #                            'WHERE "',
  #                            country, '" IN (', countries_string, ') AND "',
  #                            year, '" IN (', years_string, ');')
  #
  # DBI::dbExecute(conn, insert_rows_stmt)

  source_tbl <- dplyr::tbl(conn, source)
  dest_tbl <- dplyr::tbl(conn, dest)
  cnames_source <- colnames(source_tbl)
  cnames_dest <- colnames(dest_tbl)
  stopifnot(all(cnames_source %in% cnames_dest))

  # Get the fk parent tables for Country and Year
  fk_table_country <- fk_parent_tables[[country]]
  fk_table_year <- fk_parent_tables[[year]]
  # Filter each
  countryID_keep <- fk_table_country |>
    dplyr::filter(.data[[country]] %in% countries)
  yearID_keep <- fk_table_year |>
    dplyr::filter(.data[[year]] %in% years)

  filtered_source <- source_tbl |>
    dplyr::semi_join(countryID_keep, by = country) |>
    dplyr::semi_join(yearID_keep, by = year) |>
    dplyr::mutate(
      "{country}" := NULL,
      "{year}" := NULL
    ) |>
    dplyr::rename(
      "{country}" := paste0(country, pk_suffix),
      "{year}" := paste0(year, pk_suffix)
    )

  dplyr::rows_insert(x = dest_tbl,
                     y = filtered_source,
                     by = cnames_source,
                     in_place = in_place,
                     conflict = "ignore")

  # Download a hashtable of the result
  #
  # Change name to pl_inboard_filter_copy, because the function
  # is quite specific to the pipeline.

  return(TRUE)
}



