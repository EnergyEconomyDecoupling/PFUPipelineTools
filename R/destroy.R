#' Reset the CL-PFU database pipeline to original condition
#'
#' Sometimes, you just need to start over from scratch.
#' This function destroys the local `targets` cache
#' and removes all tables in the database at `conn`.
#' Be sure you know what you're doing!
#'
#' `conn`'s user must have superuser privileges.
#'
#' Because this is a very destructive function,
#' the caller must opt into behaviors.
#'
#' When `drop_tables` is `TRUE`, `destroy_cache` is implied to be `TRUE`,
#' and the targets cache is destroyed.
#'
#' At present
#'
#' @param conn A `DBI` connection to a database.
#' @param store The path to the `targets` store.
#'              Default is `targets::tar_config_get("store")`,
#'              which is normally `_targets`.
#' @param destroy_cache A boolean that tells whether to destroy the local `targets` cache.
#'                      Default is `FALSE`.
#' @param drop_tables If `TRUE`, all tables in conn are dropped.
#'                    If a character vector, tells which tables to drop.
#'                    Default is `FALSE`, meaning that no tables will be dropped.
#'
#' @return The names of tables dropped (if any) or an empty character vector when no tables are dropped.
#'
#' @export
pl_destroy <- function(conn,
                       store = targets::tar_config_get("store"),
                       destroy_cache = FALSE,
                       drop_tables = FALSE) {

  db_name <- DBI::dbGetInfo(conn)$dbname
  if (db_name == "MexerDB") {
    # MexerDB is the name of the "real" database.
    # We don't ever want to destroy MexerDB,
    # as I did in September 2024. ---MKH
    stop("**** You can't destroy MexerDB! ****")
  }

  if (destroy_cache) {
    # Delete the local targets cache
    unlink(store, recursive = TRUE)
  }

  if (is.logical(drop_tables)) {
    if (!drop_tables) {
      return(character())
    }
    if (drop_tables) {
      # drop_tables to the names of all tables
      drop_tables <- DBI::dbListTables(conn)
    }

  }
  if (length(drop_tables) == 0) {
    return(character())
  }

  all_tables_present <- DBI::dbListTables(conn)
  if (length(all_tables_present) == 0) {
    return(character())
  }

  # Only drop tables that exist
  tables_to_drop <- sapply(all_tables_present, FUN = function(this_table) {
    this_table %in% drop_tables
  })
  if (length(tables_to_drop) == 0) {
    return(character())
  }
  tables_to_drop <- tables_to_drop |>
    which() |>
    names()

  if (inherits(conn, "PqConnection")) {
    # Remove all tables in Postgres database
    tables_to_drop |>
      purrr::map(function(this_table_name) {
        DBI::dbExecute(conn,
                       statement = paste0('DROP TABLE "', this_table_name, '" CASCADE;'))
      })
  } else {
    # Remove all tables in a different type of database
    tables_to_drop |>
      purrr::map(function(this_table_name) {
        DBI::dbRemoveTable(conn, name = this_table_name)
      })
  }

  return(tables_to_drop)
}

