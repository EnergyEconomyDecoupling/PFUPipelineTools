#' Install the `compress` function to a database
#'
#' When running the pipeline,
#' we compress identical rows of the table using the version columns.
#' This plpgsql function in `compress_func_string`
#' does that work for us.
#' This function adds the `compress` function
#' to the database at `conn`.
#'
#' If `compress` is already installed into the the database at `conn`,
#' only the user who previously installed `compress` can update it.
#'
#' To remove `compress`, see [remove_compress_function()].
#'
#' @param conn A connection to the database into which the `compress`
#'             function should be added.
#' @param compress_func_string The compress function.
#'                             Default is
#'                             `readr::read_file(file.path("data-raw", "compress.sql")`.
#'
#' @returns The number of rows affected by adding the function to the
#'          database at `conn`,
#'          which should always be `0`.
#'
#' @seealso [remove_compress_function()], [compress_rows()]
#'
#' @export
install_compress_function <- function(conn,
                                      compress_func_string = system.file("extdata",
                                                                         "compress.sql",
                                                                         package = "PFUPipelineTools") |>
                                        readr::read_file()) {

  DBI::dbExecute(conn = conn,
                 statement = compress_func_string)
}


#' Remove the compress function from a database
#'
#' The database at `conn` may have the `compress` function installed
#' by `unpload_compress_function()`.
#' This `R` function removes the `compress` function from the database at `conn`.
#'
#' The function can be removed only by the owner of the function,
#' i.e., the user who installed the function in the first instance.
#'
#' @param conn A connection to the database from which the `compress`
#'             function should be removed.
#'
#' @returns The number of rows affected by adding the function to the
#'          database at `conn`,
#'          which should always be `0`.
#'
#' @seealso [install_compress_function()], [compress_rows()]
#'
#' @export
remove_compress_function <- function(conn) {
  DBI::dbExecute(conn = conn,
                 statement = "DROP PROCEDURE compress")
}


#' Execute the compress function on a database table
#'
#' During the execution of the pipeline,
#' we compress the rows of a table if identical data
#' exist for multiple versions.
#' The `compress` function performs this task
#' in the database.
#' This `R` function is a wrapper that executes
#' `compress` in the database.
#'
#' @param db_table_name The name of the table to be compressed.
#' @param valid_from_version_colname,valid_to_version_colname Column names in `db_table_name`.
#'            Defaults are
#'            `PFUPipelineTools::dataset_info$valid_from_version_colname`
#'            and
#'            `PFUPipelineTools::dataset_info$valid_to_version_colname`.
#' @param conn A connection to the database in which tables are to be compressed.
#'
#' @returns The number of rows affected by the call to `compress`
#'          `db_table_name`, which should always be `0`.
#'
#' @seealso [install_compress_function()], [remove_compress_function()]
#'
#' @export
compress_rows <- function(db_table_name,
                          valid_from_version_colname = PFUPipelineTools::dataset_info$valid_from_version_colname,
                          valid_to_version_colname = PFUPipelineTools::dataset_info$valid_to_version_colname,
                          conn) {
  sql_stmt <- paste0("CALL compress('",
                     db_table_name, "', '",
                     valid_from_version_colname, "', '",
                     valid_to_version_colname, "'",
                     ");")
  DBI::dbExecute(conn = conn, statement = sql_stmt)
}
