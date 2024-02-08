#' Reset the CL-PFU database pipeline to original condition
#'
#' Sometimes, you just need to start over from scratch.
#' This function destroys the local `targets` cache
#' and removes all tables in the database at `conn`.
#' Be sure you know what you're doing!
#'
#' @param conn A `DBI` connection to a database.
#'             Default uses the value of `conn_params` to connect.
#'             Override with your own connection.
#' @param conn_params A named list with items `dbname`, `host`, `port`, and `user`,
#'                    corresponding to the parameters for `DBI::dbConnect()`.
#' @param store The path to the `targets` store.
#'              Default is `targets::tar_config_get("store")`,
#'              which is normally `_targets`.
#'
#' @return If successful, `TRUE`, otherwise, an error is thrown.
#'
#' @export
pl_destroy <- function(conn = DBI::dbConnect(drv = RPostgres::Postgres(),
                                             dbname = conn_args$dbname,
                                             host = conn_args$host,
                                             port = conn_args$port,
                                             user = conn_args$user),
                       conn_params,
                       store = targets::tar_config_get("store")) {

  # Destroy the local targets cache
  targets::tar_destroy(verbose = FALSE, ask = FALSE, store = store)

  # Remove all tables in the database
  DBI::dbListTables(conn) |>
    purrr::map(function(this_table) {
      DBI::dbRemoveTable(conn, this_table)
    })

  return(TRUE)
}
