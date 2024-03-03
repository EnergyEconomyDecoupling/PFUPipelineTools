#' Download a data frame based on its `pl_hash()`
#'
#' @param hashed_table A table created by `pl_hash()`.
#' @param conn The database connection.
#' @param decode_fks A boolean that tells whether to decode foreign keys
#'                   before returning.
#'                   Default is `TRUE`.
#' @param retain_table_name_col A boolean that tells whether to retain the
#'                              table name column (`.table_name_col`).
#'                              Default is `FALSE`.
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
pl_collect_from_hash <- function(hashed_table,
                                 conn,
                                 decode_fks = TRUE,
                                 retain_table_name_col = FALSE,
                                 schema = schema_from_conn(conn = conn),
                                 fk_parent_tables = get_all_fk_tables(conn = conn, schema = schema),
                                 .table_name_col = PFUPipelineTools::hashed_table_colnames$db_table_name,
                                 .nested_hash_col = PFUPipelineTools::hashed_table_colnames$nested_hash_col_name) {
  table_name <- hashed_table |>
    dplyr::ungroup() |>
    dplyr::select(dplyr::all_of(.table_name_col)) |>
    unlist() |>
    unname() |>
    unique()
  assertthat::assert_that(length(table_name) == 1,
                          msg = "More than 1 table received in pl_collect_from_hash()")
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
  if (retain_table_name_col) {
    out <- out |>
      dplyr::mutate(
        "{.table_name_col}" := table_name
      ) |>
      # Move the table name column to the left
      dplyr::relocate(dplyr::all_of(.table_name_col))
  }
  return(out)
}


#' Collect a table from the database with natural filtering
#'
#' Often when collecting data from the database,
#' filtering is desired.
#' But filtering based on foreign keys is effectively impossible,
#' because the foreign keys are encoded.
#' This function translates natural filter commands using foreign key values to
#' filter commands using foreign keys,
#' thereby smoothing the download and filtering process.
#'
#' `schema` is a data model (`dm` object) for the CL-PFU database.
#' It can be obtained from calling `schema_from_conn()`.
#'
#' `fk_parent_tables` is a named list of tables,
#' some of which are fk parent tables containing
#' the mapping between fk values (usually strings)
#' and fk keys (usually integers)
#' for `db_table_name`.
#' `fk_parent_tables` is treated as a store from which foreign key tables
#' are retrieved by name when needed.
#' An appropriate value for `fk_parent_tables` can be obtained
#' from `get_all_fk_tables()`.
#'
#' @param db_table_name The name of a database table.
#' @param ... Natural filtering commands, such as would be applied
#'            in the `...` argument of `dplyr::filter()`.
#' @param conn The database connection.
#' @param schema The data model (`dm` object) for the database in `conn`.
#'               See details.
#' @param fk_parent_tables A named list of all parent tables
#'                         for the foreign keys in `db_table_name`.
#'                         See details.
#'
#' @return A data frame downloaded from `conn`, a filtered version of `db_table_name`.
#'
#' @export
pl_filter_collect <- function(db_table_name, ..., conn,
                              schema = schema_from_conn(conn = conn),
                              fk_parent_tables = get_all_fk_tables(conn = conn, schema = schema)) {
  dplyr::tbl(conn, db_table_name) |>
    decode_fks(db_table_name = db_table_name,
               schema = schema,
               fk_parent_tables = fk_parent_tables) |>
    dplyr::filter(!!!rlang::enquos(...)) |>
    dplyr::collect()
}


vars_in_dots <- function(enquos_dots) {
  enquos_dots |>
    # as.character(enquos_dots) turns the quosure into a string
    # in which table column names are prefixed by "~" and
    # terminated with a white space.
    as.character() |>
    # This pattern extracts for all characters between "~" and whitespace,
    # namely the column names to be decoded.
    stringr::str_extract("(?<=~)([^\\s]+)") |>
    # Make sure we have no duplicates.
    unique()
}
