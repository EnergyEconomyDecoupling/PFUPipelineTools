#' Download a data frame based on its `pl_hash()`
#'
#' @param hashed_table A table created by `pl_hash()`.
#' @param conn The database connection.
#' @param .table_name_col,.nested_col  See `PFUPipelineTools::hashed_table_colnames`.
#'
#' @return The downloaded data frame described by `hashed_table`.
#'
#' @export
pl_collect <- function(hashed_table,
                       conn,
                       .table_name_col = PFUPipelineTools::hashed_table_colnames$db_table_name,
                       .nested_hash_col = PFUPipelineTools::hashed_table_colnames$nested_hash_col_name) {
  table_name <- hashed_table |>
    dplyr::ungroup() |>
    dplyr::select(dplyr::all_of(.table_name_col)) |>
    unlist() |>
    unname()
  assertthat::assert_that(length(table_name) == 1,
                          msg = "More than 1 table received in pl_collect()")
  filter_tbl <- hashed_table |>
    dplyr::select(!dplyr::all_of(c(.table_name_col, .nested_hash_col)))
  dplyr::tbl(conn, table_name) |>
    # Perform a semi_join to keep only the rows in x that have a match in y
    dplyr::semi_join(filter_tbl, copy = TRUE, by = colnames(filter_tbl)) |>
    dplyr::collect()
}


