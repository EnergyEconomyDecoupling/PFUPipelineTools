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
#' @param x The character vector or list of characters
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
#' for outboard processing purposes,
#' especially when converting strings to integer keys.
#' This function extracts a named list of data frames,
#' where each data frame is a foreign key table in the database.
#' The data frame items in the list are named
#' by the names of the tables in the database at `conn`.
#'
#' `schema` is a data model (`dm` object) for the database in `conn`.
#' Its default value (`dm::dm_from_con(conn, learn_keys = TRUE)`)
#' extracts the data model for the database at `conn` automatically.
#' However, if the caller already has the data model,
#' supplying it in the `schema` argument will save some time.
#'
#' @param schema The data model (`dm` object) for the database in `conn`.
#'               Default is `dm::dm_from_con(conn, learn_keys = TRUE)`.
#'               See details.
#' @param conn A connection to a database.
#'
#' @return A named list of data frames,
#'         where each data frame
#'         is a table in `conn` that contains
#'         foreign keys and their values.
#' @export
get_all_fk_tables <- function(schema = dm::dm_from_con(conn, learn_keys = TRUE),
                              conn) {
  schema |>
    dm::dm_get_all_fks() |>
    magrittr::extract2("parent_table") |>
    unique() |>
    self_name() |>
    lapply(FUN = function(this_parent_table) {
      dplyr::tbl(conn, this_parent_table) |>
        dplyr::collect()
    })
}
