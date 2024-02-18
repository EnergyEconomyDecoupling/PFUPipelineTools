#' Download a data frame based on its `pl_hash()`
#'
#' @param hashed_table A table created by `pl_hash()`.
#' @param decode_fks A boolean that tells whether to decode foreign keys
#'                   before returning.
#'                   Default is `TRUE`.
#' @param conn The database connection.
#' @param schema The database schema (a `dm` object).
#'               Default calls `schema_from_conn()`, but
#'               you can supply a pre-computed schema for speed.
#'               Needed only when `decode_fks = TRUE` (the default).
#'               If foreign keys are not being decoded,
#'               setting `NULL` may improve speed.
#' @param fk_parent_tables Foreign key parent tables to assist decoding
#'                         foreign keys (when `decode_fks = TRUE`, the default).
#'                         Default calls `get_all_fk_tables()`.
#'                         Needed only when `decode_fks = TRUE` (the default).
#'                         If foreign keys are not being decoded,
#'                         setting `NULL` may improve speed.
#' @param .table_name_col,.nested_hash_col  See `PFUPipelineTools::hashed_table_colnames`.
#'
#' @return The downloaded data frame described by `hashed_table`.
#'
#' @export
pl_collect <- function(hashed_table,
                       decode_fks = TRUE,
                       conn,
                       schema = schema_from_conn(conn = conn),
                       fk_parent_tables = get_all_fk_tables(conn = conn, schema = schema),
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
  out <- dplyr::tbl(conn, table_name)
  if (ncol(filter_tbl) > 0) {
    out <- out |>
      # Perform a semi_join to keep only the rows in x that have a match in y
      dplyr::semi_join(filter_tbl, copy = TRUE, by = colnames(filter_tbl))
  }
  out <- out |>
    dplyr::collect()
  if (decode_fks) {
    out <- out |>
      decode_fks(db_table_name = table_name,
                 schema = schema,
                 fk_parent_tables = fk_parent_tables)
  }
  return(out)
}


