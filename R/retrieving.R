#' Download a data frame based on its `pl_hash()`
#'
#' If `hashed_table` has `0` rows, `NULL` is returned.
#'
#' @param hashed_table A table created by `pl_hash()`.
#' @param version_string A string of length 1 indicating the version to be downloaded.
#'                       `NULL`, the default, means to download all versions.
#' @param decode_fks A boolean that tells whether to decode foreign keys
#'                   before returning.
#'                   Default is `TRUE`.
#' @param retain_table_name_col A boolean that tells whether to retain the
#'                              table name column (`.table_name_col`).
#'                              Default is `FALSE`.
#' @param set_tar_group A boolean that tells whether to set the
#'                      `tar_group_colname` column of the output to
#'                      the same value as the input.
#'                      There can be only one unique value in `tar_group_colname`,
#'                      otherwise an error is raised.
#'                      Default is `TRUE`.
#' @param decode_matsindf A boolean that tells whether to decode the
#'                        a matsindf data frame.
#'                        Calls [decode_matsindf()] internally.
#'                        Default is `TRUE`.
#' @param index_map_name The name of the index map.
#'                       Default is "Index".
#' @param index_map A list of data frames to assist with decoding matrices.
#'                  Passed to [decode_matsindf()] when `decode_matsindf` is `TRUE`
#'                  but otherwise not needed.
#'                  Default is `fk_parent_tables[[index_map_name]]`.
#' @param rctype_table_name The name of the row and column types.
#' @param rctypes A data frame of row and column types.
#'                Passed to [decode_matsindf()] when `decode_matsindf` is `TRUE`
#'                but otherwise not needed.
#'                Default calls [decode_fks()].
#' @param matrix_class One of "Matrix" (the default for sparse matrices)
#'                     or ("matrix") for the native matrix form in `R`.
#'                     Default is "Matrix".
#' @param tar_group_colname The name of the `tar_group` column.
#'                          default is `PFUPipelineTools::hashed_table_colnames$tar_group_colname`.
#' @param matname_colname,matval_colname Names used for matrix names and matrix values.
#'                                       Defaults are
#'                                       `PFUPipelineTools::mat_meta_cols$matname` and
#'                                       `PFUPipelineTools::mat_meta_cols$matval`,
#'                                       respectively.
#' @param valid_from_version_colname,valid_to_version_colname Names
#'              for columns containing version information.
#'              Defaults are
#'              `PFUPipelineTools::dataset_info$valid_from_version`
#'              and
#'              `PFUPipelineTools::dataset_info$valid_to_version`,
#'              respectively.
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
#'                         setting to `NULL` may improve speed.
#' @param .table_name_col,.nested_hash_col  See `PFUPipelineTools::hashed_table_colnames`.
#'
#' @return The downloaded data frame described by `hashed_table`.
#'
#' @export
pl_collect_from_hash <- function(hashed_table,
                                 version_string = NULL,
                                 decode_fks = TRUE,
                                 retain_table_name_col = FALSE,
                                 set_tar_group = TRUE,
                                 decode_matsindf = TRUE,
                                 matrix_class = c("Matrix", "matrix"),
                                 tar_group_colname = PFUPipelineTools::hashed_table_colnames$tar_group_colname,
                                 matname_colname = PFUPipelineTools::mat_meta_cols$matname,
                                 matval_colname = PFUPipelineTools::mat_meta_cols$matval,
                                 valid_from_version_colname = PFUPipelineTools::dataset_info$valid_from_version_colname,
                                 valid_to_version_colname = PFUPipelineTools::dataset_info$valid_to_version_colname,
                                 conn,
                                 schema = schema_from_conn(conn = conn),
                                 fk_parent_tables = get_all_fk_tables(conn = conn, schema = schema),
                                 index_map_name = "Index",
                                 index_map = fk_parent_tables[[index_map_name]],
                                 rctype_table_name = "matnameRCType",
                                 rctypes = decode_fks(db_table_name = rctype_table_name,
                                                      collect = TRUE,
                                                      conn = conn,
                                                      schema = schema,
                                                      fk_parent_tables = fk_parent_tables),
                                 .table_name_col = PFUPipelineTools::hashed_table_colnames$db_table_name,
                                 .nested_hash_col = PFUPipelineTools::hashed_table_colnames$nested_hash_colname) {

  matrix_class <- match.arg(matrix_class)
  if (nrow(hashed_table) == 0) {
    return(NULL)
  }
  table_name <- hashed_table |>
    dplyr::ungroup() |>
    dplyr::select(dplyr::all_of(.table_name_col)) |>
    unlist() |>
    unname() |>
    unique()
  assertthat::assert_that(length(table_name) == 1,
                          msg = "More than 1 table received in pl_collect_from_hash()")

  out <- dplyr::tbl(conn, table_name)

  # Filter on any foreign keys
  filter_tbl <- hashed_table |>
    PFUPipelineTools::tar_ungroup() |>
    dplyr::select(!dplyr::all_of(c(.table_name_col, .nested_hash_col))) |>
    # Need to encode foreign keys, because the table in the database has
    # encoded foreign keys
    encode_fks(db_table_name = table_name,
               schema = schema,
               fk_parent_tables = fk_parent_tables)

  if (ncol(filter_tbl) > 0) {
    out <- out |>
      # Perform a semi_join to keep only the rows in x that have a match in y
      dplyr::semi_join(filter_tbl, copy = TRUE, by = colnames(filter_tbl))
  }

  # Filter on the version, if requested
  if (!is.null(version_string)) {
    out <- out |>
      filter_on_version_string(version_string = version_string,
                               db_table_name = table_name,
                               collect = FALSE,
                               conn = conn,
                               schema = schema,
                               fk_parent_tables = fk_parent_tables,
                               valid_from_version_colname = valid_from_version_colname,
                               valid_to_version_colname = valid_to_version_colname)
  }

  out <- out |>
    dplyr::collect()
  if (decode_fks) {
    out <- out |>
      decode_fks(db_table_name = table_name,
                 schema = schema,
                 fk_parent_tables = fk_parent_tables)
  }
  if (decode_matsindf) {
    out <- out |>
      decode_matsindf(index_map = index_map,
                      rctypes = rctypes,
                      matrix_class = matrix_class)
  }
  if (retain_table_name_col) {
    out <- out |>
      dplyr::mutate(
        "{.table_name_col}" := table_name
      ) |>
      # Move the table name column to the left
      dplyr::relocate(dplyr::all_of(.table_name_col))
  }
  if (set_tar_group & (tar_group_colname %in% colnames(hashed_table))) {
    # Get the tar_group
    this_tar_group <- hashed_table[[tar_group_colname]] |>
      unique()
    assertthat::assert_that(length(this_tar_group) == 1,
                            msg = "You asked for the tar_group column to be retained, but there is more than 1 tar_group in `hashed_table`")
    out <- out |>
      dplyr::mutate(
        "{tar_group_colname}" := this_tar_group
      )
  }
  return(out)
}


#' Filter a table from the database using natural expressions
#'
#' Often when collecting data from the database,
#' filtering is desired.
#' But filtering based on foreign keys
#' (fks, as stored in the database)
#' is effectively impossible, because of foreign key encoding.
#' This function filters based on
#' fk values (typically strings),
#' not fk keys (typically integers),
#' thereby simplifying the filtering process,
#' with optional downloading thereafter.
#' By default (`collect = FALSE`),
#' a `tbl` is returned
#' (and data are not downloaded from the database).
#' Use `dplyr::collect()` to execute the resulting SQL query
#' and obtain an in-memory data frame.
#' Or, set `collect = TRUE` to execute the SQL and
#' return an in-memory data frame.
#'
#' `schema` is a data model (`dm` object) for the CL-PFU database.
#' It can be obtained from calling `schema_from_conn()`.
#' If minimal interaction with the database is desired,
#' be sure to override the default value for `schema`
#' by supplying a pre-computed `dm` object.
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
#' If minimal interaction with the database is desired,
#' be sure to override the default value for `fk_parent_tables`
#' by supplying a pre-computed named list of
#' foreign key tables.
#'
#' @param db_table_name The string name of the database table to be filtered.
#' @param ... Filter conditions on the data frame,
#'            such as `Country == "USA"` or `Year %in% 1960:2020`.
#'            These conditions reduce data volume,
#'            because they are applied prior to downloading from `conn`.
#'            If no rows match these conditions,
#'            a data frame with no rows is returned.
#' @param version_string A string of length `1` or more
#'                       that indicates the desired version(s).
#'                       `NULL`, the default, means to download all versions available in
#'                       `db_table_name`.
#'                       `c()` (an empty string) returns a zero-row table.
#'                       If `version_string` is invalid, an error will be emitted.
#' @param collect A boolean that tells whether to download the result.
#'                Default is `FALSE`.
#'                See details.
#' @param create_matsindf A boolean that tells whether to create a matsindf data frame
#'                        from the collected data frame.
#'                        Default is the value of `collect`,
#'                        such that setting `collect = TRUE` also
#'                        implies `create_matsindf`.
#' @param conn The database connection.
#' @param schema The data model (`dm` object) for the database in `conn`.
#'               Default is `schema_from_conn(conn = conn)`.
#'               See details.
#' @param fk_parent_tables A named list of all parent tables
#'                         for the foreign keys in `db_table_name`.
#'                         Default is
#'                         `get_all_fk_tables(conn = conn, schema = schema)`.
#'                         See details.
#' @param index_map_name The name of the table that serves as the index for row and column names.
#'                       Default is "Index".
#' @param index_map The index map for the matrices in the database at `conn`.
#'                  Default is `fk_parent_tables[[index_table_name]]`.
#' @param rctype_table_name The name of the table that contains row and column types.
#'                          Default is "matnameRCType".
#' @param rctypes The table of row and column types for the database at `conn`.
#'                Default is `fk_parent_tables[[rctype_table_name]]`.
#' @param matrix_class One of "Matrix" (the default) for sparse matrices or
#'                     "matrix" (the base matrix representation in `R`) for non-sparse matrices.
#' @param matname The name of the matrix name column.
#'                Default is `PFUPipelineTools::mat_meta_cols$matname`.
#' @param matval The name of the matrix value column.
#'               Default is `PFUPipelineTools::mat_meta_cols$matval`.
#' @param rowtype_colname,coltype_colname The names for row and column type columns in data frames.
#'                                        Defaults are
#'                                        `PFUPipelineTools::mat_meta_cols$rowtype` and
#'                                        `PFUPipelineTools::mat_meta_cols$coltype`,
#'                                        respectively.
#' @param valid_from_version_colname,valid_to_version_colname Names
#'              for columns containing version information.
#'              Defaults are
#'              `PFUPipelineTools::dataset_info$valid_from_version_colname`
#'              and
#'              `PFUPipelineTools::dataset_info$valid_to_version_colname`,
#'              respectively.
#'
#' @return A filtered version of `db_table_name` downloaded from `conn`.
#'
#' @export
pl_filter_collect <- function(db_table_name,
                              ...,
                              version_string = NULL,
                              collect = FALSE,
                              create_matsindf = collect,
                              conn,
                              schema = schema_from_conn(conn = conn),
                              fk_parent_tables = get_all_fk_tables(conn = conn, schema = schema),
                              index_map_name = "Index",
                              index_map = fk_parent_tables[[index_map_name]],
                              rctype_table_name = "matnameRCType",
                              rctypes = decode_fks(db_table_name = rctype_table_name,
                                                   collect = TRUE,
                                                   conn = conn,
                                                   schema = schema,
                                                   fk_parent_tables = fk_parent_tables),
                              matrix_class = c("Matrix", "matrix"),
                              matname = PFUPipelineTools::mat_meta_cols$matname,
                              matval = PFUPipelineTools::mat_meta_cols$matval,
                              rowtype_colname = PFUPipelineTools::mat_meta_cols$rowtype,
                              coltype_colname = PFUPipelineTools::mat_meta_cols$coltype,
                              valid_from_version_colname = PFUPipelineTools::dataset_info$valid_from_version_colname,
                              valid_to_version_colname = PFUPipelineTools::dataset_info$valid_to_version_colname) {

  matrix_class <- match.arg(matrix_class)

  out <- dplyr::tbl(src = conn, db_table_name)

  # First, filter the tbl according to version string,
  # if desired.
  if (!is.null(version_string)) {
    out <- out |>
      filter_on_version_string(version_string = version_string,
                               db_table_name = db_table_name,
                               schema = schema,
                               fk_parent_tables = fk_parent_tables,
                               valid_from_version_colname = valid_from_version_colname,
                               valid_to_version_colname = valid_to_version_colname)
  }

  # Next, decode the foreign keys in the tbl with
  # collect = FALSE to ensure a tbl is returned.
  out <- out |>
    decode_fks(db_table_name = db_table_name,
               schema = schema,
               fk_parent_tables = fk_parent_tables,
               collect = FALSE)


  # Finally, filter the foreign keys in the tbl based on the expressions in ...
  filter_args <- rlang::enquos(...)
  out <- out |>
    dplyr::filter(!!!filter_args)

  if (collect) {
    # Collect (execute the SQL), if desired.
    out <- out |>
      dplyr::collect()
  }

  if (create_matsindf) {
    out <- out |>
      # Now decode the matsindf data frame
      decode_matsindf(index_map = index_map,
                      rctypes = rctypes,
                      matrix_class = matrix_class,
                      matname = matname,
                      matval = matval,
                      rowtype_colname = rowtype_colname,
                      coltype_colname = coltype_colname)
  }

  return(out)
}
