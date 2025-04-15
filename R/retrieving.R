#' Download a data frame based on its `pl_hash()`
#'
#' If `hashed_table` has `0` rows, `NULL` is returned.
#'
#' @param hashed_table A table created by `pl_hash()`.
#' @param version A string indicating the version to be downloaded.
#'                `NULL`, the default, means to download all versions.
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
#' @param index_map A list of data frames to assist with decoding matrices.
#'                  Passed to [decode_matsindf()] when `decode_matsindf` is `TRUE`
#'                  but otherwise not needed.
#' @param rctypes A data frame of row and column types.
#'                Passed to [decode_matsindf()] when `decode_matsindf` is `TRUE`
#'                but otherwise not needed.
#' @param matrix_class One of "Matrix" (the default for sparse matrices)
#'                     or ("matrix") for the native matrix form in `R`.
#'                     Default is "Matrix".
#' @param tar_group_colname The name of the `tar_group` column.
#'                          default is `PFUPipelineTools::hashed_table_colnames$tar_group_colname`.
#' @param matname_colname,matval_colname Names used for matrix names and matrix values.
#'                                       Defaults are
#'                                       `PFUPipelineTools::mat_meta_cols$matname` and
#'                                       `PFUPipelineTools::mat_meta_cols$matval`, respectively.
#' @param valid_from_version_colname,valid_to_version_colname Names for columns containing version information.
#'                                                            Defaults are
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
                                 version = NULL,
                                 decode_fks = TRUE,
                                 retain_table_name_col = FALSE,
                                 set_tar_group = TRUE,
                                 decode_matsindf = TRUE,
                                 matrix_class = c("Matrix", "matrix"),
                                 tar_group_colname = PFUPipelineTools::hashed_table_colnames$tar_group_colname,
                                 matname_colname = PFUPipelineTools::mat_meta_cols$matname,
                                 matval_colname = PFUPipelineTools::mat_meta_cols$matval,
                                 valid_from_version_colname = PFUPipelineTools::version_cols$valid_from_version,
                                 valid_to_version_colname = PFUPipelineTools::version_cols$valid_to_version,
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
  filter_tbl <- hashed_table |>
    PFUPipelineTools::tar_ungroup() |>
    dplyr::select(!dplyr::all_of(c(.table_name_col, .nested_hash_col))) |>
    # Need to encode foreign keys, because the table in the database has
    # encoded foreign keys
    encode_fks(db_table_name = table_name,
               conn = conn,
               schema = schema,
               fk_parent_tables = fk_parent_tables)
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
#' But filtering based on foreign keys (as stored in the database)
#' is effectively impossible, because of foreign key encoding.
#' This function filters based on
#' fk values (typically strings),
#' not fk keys (typically integers),
#' thereby simplifying the filtering process,
#' with optional downloading thereafter.
#' By default, a `tbl` is returned
#' (and data are not downloaded from the database).
#' Use `dplyr::collect()` to execute the resulting SQL query
#' and obtain an in-memory data frame.
#' Or, set `collect = TRUE` to execute the SQL and
#' return an in-memory data frame.
#'
#' To obtain `db_table_name` _without_ filtering
#' but with fk keys (typically integers)
#' decoded to fk values (typically strings),
#' call this function with nothing in the `...` argument.
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
#' Setting any of the filtering arguments
#' (`countries`, `years`, `methods`, `last_stages`, `energy_types`, `includes_neu`)
#' to `NULL` turns off filtering and returns all values.
#'
#' When `collect = TRUE`,
#' [decode_matsindf()] is called on the downloaded data frame.
#'
#' @param db_table_name The string name of the database table to be filtered.
#' @param datasets A vector of dataset strings to be retained in the output.
#'                 `NULL` (the default) returns all datasets.
#'                 Another interesting option is
#'                 `PFUPipelineTools::dataset_info$clpfu_iea`.
#' @param countries A vector of country strings to be retained in the output.
#'                  `NULL` (the default) returns all countries.
#'                  Another useful option is `as.character(PFUPipelineTools::canonical_countries)`.
#' @param years A vector of integers to be retained in the output.
#'              `NULL` (the default) returns all years.
#'              Another option is `1960:2020`.
#' @param methods A vector of method strings to be retained in the output.
#'                `NULL` (the default) returns all methods.
#'                Another good option is "PCM" (physical content method).
#' @param last_stages A vector of last stage strings to be retained in the output.
#'                    At present, only "Final" and "Useful" are implemented.
#'                    `NULL` (the default) returns all last stages.
#' @param energy_types A vector of energy type strings to be retained in the output.
#'                     At present, only "E" (energy) and "X" (exergy) are implemented.
#'                     `NULL` (the default) returns all energy types.
#' @param gross_nets A vector of values for the `GrossNet` column (when it exists).
#'                   `NULL` (the default) turns off filtering on this column.
#'                   Other good values are "Gross" and "Net".
#' @param includes_neu A vector of booleans that indicates what to retain in output.
#'                     `TRUE` means non-energy use is included in the ECCs.
#'                     `FALSE` means non-energy use is excluded from the ECCs.
#'                     Default is `NULL`, meaning extract both `TRUE` and `FALSE values.
#' @param industry_aggs A vector of strings identifying the industry aggregations desired.
#'                     `NULL` (the default) means all industry aggregations should be returned.
#'                     Other reasonable values are "Specified", "Despecified", and "Grouped".
#' @param product_aggs A vector of strings identifying the product aggregations desired.
#'                     `NULL` (the default) means all product aggregations should be returned.
#'                     Other reasonable values are "Specified", "Despecified", and "Grouped".
#' @param chopped_mats A vector of strings identifying the matrix chops desired.
#'                     `NULL` (the default) means all chopped matrices should be returned.
#' @param chopped_vars A vector of strings identifying the matrix variables desired.
#'                     `NULL` (the default) means all chopped variables should be returned.
#' @param collect A boolean that tells whether to download the result.
#'                Default is `FALSE`.
#'                See details.
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
#'                Default is "matname".
#' @param matval The name of the matrix value column.
#'               Default is "matval".
#' @param rowtype_colname,coltype_colname The names for row and column type columns in data frames.
#'                                        Defaults are "rowtype" and "coltype", respectively.
#' @param dataset_colname,country_colname,year_colname,method_colname,last_stage_colname,energy_type_colname,gross_net_colname,product_agg_colname,industry_agg_colname,chopped_mat_colname,chopped_var_colname Columns that are likely to be in db_table_name
#'                                                                             and may be filtered with `%in%`-style subsetting.
#' @param includes_neu_col The name of a column that tells whether non-energy
#'                         use (NEU) is included.
#'                         Default is `Recca::psut_cols$includes_neu`.
#'
#' @return A filtered version of `db_table_name` downloaded from `conn`.
#'
#' @export
pl_filter_collect <- function(db_table_name,
                              datasets = NULL,
                              countries = NULL,
                              years = NULL,
                              methods = NULL,
                              last_stages = NULL,
                              energy_types = NULL,
                              gross_nets = NULL,
                              includes_neu = NULL,
                              industry_aggs = NULL,
                              product_aggs = NULL,
                              chopped_mats = NULL,
                              chopped_vars = NULL,
                              collect = FALSE,
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
                              matname = "matname",
                              matval = "matval",
                              rowtype_colname = "rowtype",
                              coltype_colname = "coltype",
                              country_colname = IEATools::iea_cols$country,
                              year_colname = IEATools::iea_cols$year,
                              method_colname = IEATools::iea_cols$method,
                              last_stage_colname = IEATools::iea_cols$last_stage,
                              energy_type_colname = IEATools::iea_cols$energy_type,
                              gross_net_colname = Recca::efficiency_cols$gross_net,
                              dataset_colname = PFUPipelineTools::dataset_info$dataset_colname,
                              includes_neu_colname = Recca::psut_cols$includes_neu,
                              product_agg_colname = PFUPipelineTools::aggregation_df_cols$product_aggregation,
                              industry_agg_colname =  PFUPipelineTools::aggregation_df_cols$industry_aggregation,
                              chopped_mat_colname = PFUPipelineTools::aggregation_df_cols$chopped_mat,
                              chopped_var_colname = PFUPipelineTools::aggregation_df_cols$chopped_var) {

  matrix_class <- match.arg(matrix_class)

  out <- dplyr::tbl(src = conn, db_table_name) |>
    # First, decode the foreign keys with
    # collect = FALSE to ensure a tbl is returned.
    decode_fks(db_table_name = db_table_name,
               schema = schema,
               fk_parent_tables = fk_parent_tables,
               collect = FALSE)

  cnames <- colnames(out)
  if (!is.null(datasets) & dataset_colname %in% cnames) {
    # %in% seemingly works only on vectors, not lists
    # And the vectors need to be previously defined.
    # They can't appear inline as unlist(X)
    dsets <- unlist(datasets)
    out <- out |>
      dplyr::filter(.data[[dataset_colname]] %in% dsets)
  }
  if (!is.null(countries) & country_colname %in% cnames) {
    couns <- unlist(countries)
    out <- out |>
      dplyr::filter(.data[[country_colname]] %in% couns)
  }
  if (!is.null(years) & year_colname %in% cnames) {
    yrs <- unlist(years)
    out <- out |>
      dplyr::filter(.data[[year_colname]] %in% yrs)
  }
  if (!is.null(methods) & method_colname %in% cnames) {
    meths <- unlist(methods)
    out <- out |>
      dplyr::filter(.data[[method_colname]] %in% meths)
  }
  if (!is.null(last_stages) & last_stage_colname %in% cnames) {
    lstages <- unlist(last_stages)
    out <- out |>
      dplyr::filter(.data[[last_stage_colname]] %in% lstages)
  }
  if (!is.null(energy_types) & energy_type_colname %in% cnames) {
    etypes <- unlist(energy_types)
    out <- out |>
      dplyr::filter(.data[[energy_type_colname]] %in% etypes)
  }
  if (!is.null(gross_nets) & gross_net_colname %in% cnames) {
    grossnettypes <- unlist(gross_nets)
    out <- out |>
      dplyr::filter(.data[[gross_net_colname]] %in% grossnettypes)
  }
  if (!is.null(includes_neu) & includes_neu_colname %in% cnames) {
    ineu <- unlist(includes_neu)
    out <- out |>
      dplyr::filter(.data[[includes_neu_colname]] %in% ineu)
  }
  if (!is.null(industry_aggs) & industry_agg_colname %in% cnames) {
    indaggs <- unlist(industry_aggs)
    out <- out |>
      dplyr::filter(.data[[industry_agg_colname]] %in% indaggs)
  }
  if (!is.null(product_aggs) & product_agg_colname %in% cnames) {
    prodaggs <- unlist(product_aggs)
    out <- out |>
      dplyr::filter(.data[[product_agg_colname]] %in% prodaggs)
  }
  if (!is.null(chopped_mats) & chopped_mat_colname %in% cnames) {
    chopped_mats <- unlist(chopped_mats)
    out <- out |>
      dplyr::filter(.data[[chopped_mat_colname]] %in% chopped_mats)
  }
  if (!is.null(chopped_vars) & chopped_var_colname %in% cnames) {
    chopped_vars <- unlist(chopped_vars)
    out <- out |>
      dplyr::filter(.data[[chopped_var_colname]] %in% chopped_vars)
  }

  if (collect) {
    # Collect (execute the SQL), if desired.
    out <- out |>
      dplyr::collect() |>
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


# vars_in_dots <- function(enquos_dots) {
#   enquos_dots |>
#     # as.character(enquos_dots) turns the quosure into a string
#     # in which table column names are prefixed by "~" and
#     # terminated with a white space.
#     as.character() |>
#     # This pattern extracts for all characters between "~" and whitespace,
#     # namely the column names to be decoded.
#     stringr::str_extract("(?<=~)([^\\s]+)") |>
#     # Make sure we have no duplicates.
#     unique()
# }
