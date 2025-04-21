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
#' @param learn_keys A boolean that tells whether to include the definition
#'                   of primary and final keys in the return value.
#'                   Default is `TRUE`.
#'
#' @return A `dm` object.
#'
#' @export
schema_from_conn <- function(conn = NULL,
                             table_names = NULL,
                             learn_keys = TRUE) {
  dm::dm_from_con(conn,
                  table_names = table_names,
                  learn_keys = TRUE)
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
#' The `dependencies` argument can be a vector of other objects
#' upon which the desired inboard copy depends.
#' Typically, the target that makes `source` should be given in `dependencies`,
#' for unless the target that makes `source` completes,
#' the inboard filter copy will fail.
#' `dependencies` is ignored internally.
#'
#' `schema` is a data model (`dm` object) for the database in `conn`.
#' Its default value (`schema_from_conn(conn)`)
#' extracts the data model for the database at `conn` automatically.
#' However, if the caller already has the data model,
#' supplying it in the `schema` argument will save time.
#'
#' `fk_parent_tables` is a named list of tables,
#' one of which (the one named `db_table_name`)
#' contains the foreign keys for `db_table_name`.
#' `fk_parent_tables` is treated as a store from which foreign key tables
#' are retrieved by name when needed.
#' The default value (which calls `get_all_fk_tables()`
#' with `collect = TRUE` because decoding of foreign keys
#' is done outboard of the database)
#' retrieves all possible foreign key parent tables from `conn`,
#' potentially a time-consuming process.
#' For speed, pre-compute all foreign key parent tables once
#' (via `get_all_fk_tables(collect = TRUE)`)
#' and pass the list to the `fk_parent_tables` argument
#' of this function.
#'
#' @param source A string identifying the source table.
#' @param dest A string identifying the destination table.
#' @param countries Countries to keep.
#' @param years Years to keep.
#' @param empty_dest A boolean that tells whether to empty the destination table before copying.
#'                   Default is `TRUE`.
#' @param in_place A boolean that tells whether to make the changes in the remote
#'                 database at `conn`.
#' @param dependencies Other objects (often targets) upon which the inboard copy depends.
#'                     The default is `NULL`.
#'                     See details.
#' @param additional_hash_group_cols A vector of strings that gives names of additional
#'                                   columns that should _not_ be hashed.
#' @param usual_hash_group_cols A string vector that gives typical names of columns that should _not_
#'                              be hashed.
#'                              Default is `PFUPipelineTools::usual_hash_group_cols`.
#' @param conn A database connection.
#' @param schema The data model (`dm` object) for the database in `conn`.
#'               Default is `dm_from_con(conn, learn_keys = TRUE)`.
#'               See details.
#' @param fk_parent_tables A named list of all parent tables
#'                         for the foreign keys in `db_table_name`.
#'                         See details.
#' @param country The name of the country column in `source` and `dest`.
#'                Default is `IEATools::iea_cols$country`.
#' @param year The name of the year column in `source` and `dest`.
#'             Default is `IEATools::iea_cols$year`.
#' @param pk_suffix The suffix for primary key columns.
#'                  Default is `PFUPipelineTools::key_col_info$pk_suffix`.
#'
#' @return A hash of the destination data frame
#'         created by [pl_upsert()].
#'
#' @export
inboard_filter_copy <- function(source,
                                dest,
                                countries = NULL,
                                years = NULL,
                                empty_dest = TRUE,
                                in_place = FALSE,
                                dependencies = NULL,
                                additional_hash_group_cols = NULL,
                                usual_hash_group_cols = PFUPipelineTools::usual_hash_group_cols,
                                conn,
                                schema = schema_from_conn(conn),
                                fk_parent_tables = get_all_fk_tables(conn = conn,
                                                                     schema = schema),
                                country = IEATools::iea_cols$country,
                                year = IEATools::iea_cols$year,
                                pk_suffix = PFUPipelineTools::key_col_info$pk_suffix) {

  if (empty_dest & in_place) {
    # empty_table <- dplyr::tbl(conn, dest) |>
    #   dplyr::filter(FALSE) |>
    #   dplyr::collect()
    # # Overwrite the existing table with the empty one
    # DBI::dbWriteTable(conn, name = "dest", value = empty_table, overwrite = TRUE)
    stmt <- paste0('DELETE FROM "', dest, '";')
    DBI::dbExecute(conn, stmt)
  }

  # Here is an example SQL query that will successfully
  # copy selected rows from source to dest.
  # INSERT INTO "dest"
  # SELECT *
  # FROM "source"
  # WHERE "Country" IN (1, 3) AND "Year" IN (1971, 1971);

  # Build clauses with code

  insert_into_clause <- paste0('INSERT INTO "', dest, '" ')
  select_clause <- 'SELECT * '
  from_source_clause <- paste0('FROM "', source, '" ')

  country_clause <- NULL
  if (!is.null(countries)) {
    countries_encoded <- encode_fk_values(countries,
                                          fk_table_name = country,
                                          fk_parent_tables = fk_parent_tables)
    countries_string <- paste0("(", paste0(countries_encoded, collapse = ", "), ")")

    country_clause <- paste0('"', country, '" IN ', countries_string)
  }
  years_clause <- NULL
  if (!is.null(years)) {
    years_encoded <- encode_fk_values(years,
                                      fk_table_name = year,
                                      fk_parent_tables = fk_parent_tables)
    years_string <- paste0("(", paste0(years_encoded, collapse = ", "), ")")
    years_clause <- paste0('"', year, '" IN ', years_string)
  }

  where_clause <- paste0('WHERE ',
                         paste0(c(country_clause, years_clause), collapse = ' AND '))

  stmt <- paste0(insert_into_clause,
                  select_clause,
                  from_source_clause,
                  where_clause)

  DBI::dbExecute(conn, stmt)

  # Download a hashed table of the dest table
  pl_hash(table_name = dest,
          conn = conn,
          additional_hash_group_cols = additional_hash_group_cols,
          usual_hash_group_cols = usual_hash_group_cols) |>
    # Decode the foreign keys, so they are human-readable.
    decode_fks(db_table_name = dest,
               schema = schema,
               fk_parent_tables = fk_parent_tables)
}


#' Encode a vector of foreign key values according to a foreign key parent table
#'
#' In a database, there are foreign key tables
#' containing fk values (usually strings) and fk keys (usually integers).
#' This function converts a vector of fk values (strings, `v_val`)
#' into a vector fk keys (integers) according to the
#' database at `conn`.
#'
#' `schema` is a data model (`dm` object) for the database in `conn`.
#' Its default value (`schema_from_conn(conn)`)
#' extracts the data model for the database at `conn` automatically.
#' However, if the caller already has the data model,
#' supplying it in the `schema` argument will save time.
#'
#' `fk_parent_tables` is a named list of tables,
#' one of which (the one named `fk_table_name`)
#' contains the foreign keys `v`.
#' `fk_parent_tables` is treated as a store from which foreign key tables
#' are retrieved by name when needed.
#' The default value (which calls `get_all_fk_tables()`
#' with `collect = TRUE`)
#' retrieves all possible foreign key parent tables from `conn`,
#' potentially a time-consuming process.
#' For speed, pre-compute all foreign key parent tables once
#' (via `get_all_fk_tables(collect = TRUE)`)
#' and pass the list to the `fk_parent_tables` argument
#' of this function.
#'
#' @param v_val A vector of fk values (typically strings) to be converted
#'              to fk keys (typically integers).
#' @param fk_table_name The name of the foreign key table in `conn`
#'                      that contains the mapping from fk values
#'                      to fk keys.
#' @param conn A connection to the CL-PFU database.
#'             Needed only if `fk_parent_tables` is not provided.
#'             Default is `NULL`.
#' @param schema The data model (`dm` object) for the database in `conn`.
#'               Default is `dm_from_con(conn, learn_keys = TRUE)`.
#'               Needed only if `fk_parent_tables` is not provided.
#'               See details.
#' @param fk_parent_tables A named list of all parent tables
#'                         for the foreign keys in `db_table_name`.
#'                         See details.
#' @param pk_suffix A string that gives the suffix for
#'                  primary key columns.
#'                  Default is `PFUPipelineTools::key_col_info$pk_suffix`.
#'
#' @return `v_val` encoded according to `fk_parent_tables`.
#'
#' @export
#'
#' @examples
#' fk_parent_tables <- list(
#'   Country = tibble::tribble(~CountryID, ~Country,
#'                             1, "USA",
#'                             2, "ZAF",
#'                             3, "GHA"))
#' encode_fk_values(c("USA", "USA", "GHA"),
#'                  fk_table_name = "Country",
#'                  fk_parent_tables = fk_parent_tables)
encode_fk_values <- function(v_val,
                             fk_table_name,
                             conn = NULL,
                             schema = schema_from_conn(conn),
                             fk_parent_tables = get_all_fk_tables(conn = conn,
                                                                  schema = schema,
                                                                  collect = TRUE),
                             pk_suffix = PFUPipelineTools::key_col_info$pk_suffix) {
  # Establish column names in the foreign key table (fk_table_name)
  fk_value_col_in_fk_table_name <- fk_table_name
  fk_key_col_in_fk_table_name <- paste0(fk_value_col_in_fk_table_name, pk_suffix)
  # Get the foreign key parent table
  this_fk_parent_table <- fk_parent_tables[[fk_table_name]]
  # Make a data frame from v_val
  out <- data.frame(v_val) |>
    # Set the colname to be the name of the fk value column.
    magrittr::set_names(fk_value_col_in_fk_table_name) |>
    # Join with the fk parent table
    dplyr::left_join(this_fk_parent_table,
                     by = fk_value_col_in_fk_table_name,
                     copy = TRUE) |>
    # Extract the column that we want to return
    magrittr::extract2(fk_key_col_in_fk_table_name)
  # Check for errors
  errs <- which(is.na(out), arr.ind = TRUE)
  if (length(errs) != 0) {
    bad_v_val <- v_val[errs]

    err_msg <- paste0("Unknown fk values in encode_fk_values():\n",
                      paste0(bad_v_val, collapse = ", "))
    stop(err_msg)
  }
  return(out)
}


#' Decode a vector of foreign keys according to foreign key parent table
#'
#' In a database, there are foreign key tables
#' containing fk values (usually strings) and fk keys (usually integers).
#' This function converts a vector of fk keys (integers, `v_key`)
#' into a vector fk values (strings) according to the
#' database at `conn`.
#'
#' `schema` is a data model (`dm` object) for the database in `conn`.
#' Its default value (`schema_from_conn(conn)`)
#' extracts the data model for the database at `conn` automatically.
#' However, if the caller already has the data model,
#' supplying it in the `schema` argument will save time.
#'
#' `fk_parent_tables` is a named list of tables,
#' one of which (the one named `fk_table_name`)
#' contains the foreign keys `v`.
#' `fk_parent_tables` is treated as a store from which foreign key tables
#' are retrieved by name when needed.
#' The default value (which calls `get_all_fk_tables()`
#' with `collect = TRUE`)
#' retrieves all possible foreign key parent tables from `conn`,
#' potentially a time-consuming process.
#' For speed, pre-compute all foreign key parent tables once
#' (via `get_all_fk_tables(collect = TRUE)`)
#' and pass the list to the `fk_parent_tables` argument
#' of this function.
#'
#' @param v_key A vector of fk keys (typically integers) to be converted
#'          to fk values (typically strings).
#' @param fk_table_name The name of the foreign key table in `conn`
#'                      that contains the mapping from fk values
#'                      to fk keys.
#' @param conn A connection to the CL-PFU database.
#'             Needed only if `fk_parent_tables` is not provided.
#' @param schema The data model (`dm` object) for the database in `conn`.
#'               Default is `dm_from_con(conn, learn_keys = TRUE)`.
#'               Needed only if `fk_parent_tables` is not provided.
#'               See details.
#' @param fk_parent_tables A named list of all parent tables
#'                         for the foreign keys in `db_table_name`.
#'                         See details.
#' @param pk_suffix A string that gives the suffix for
#'                  primary key columns.
#'                  Default is `PFUPipelineTools::key_col_info$pk_suffix`.
#'
#' @return `v_key` decoded according to `fk_parent_tables`.
#'
#' @export
#'
#' @examples
#' fk_parent_tables <- list(
#'   Country = tibble::tribble(~CountryID, ~Country,
#'                             1, "USA",
#'                             2, "ZAF",
#'                             3, "GHA"))
#' decode_fk_keys(c(1, 1, 3),
#'                fk_table_name = "Country",
#'                fk_parent_tables = fk_parent_tables)
decode_fk_keys <- function(v_key,
                           fk_table_name,
                           conn,
                           schema = schema_from_conn(conn),
                           fk_parent_tables = get_all_fk_tables(conn = conn,
                                                                schema = schema,
                                                                collect = TRUE),
                           pk_suffix = PFUPipelineTools::key_col_info$pk_suffix) {
  # Establish column names in the foreign key table (fk_table_name)
  fk_value_col_in_fk_table_name <- fk_table_name
  fk_key_col_in_fk_table_name <- paste0(fk_value_col_in_fk_table_name, pk_suffix)
  # Get the foreign key parent table
  this_fk_parent_table <- fk_parent_tables[[fk_table_name]]
  # Make a data frame from v_key
  out <- data.frame(v_key) |>
    # Set the colname to be the name of the fk key column.
    magrittr::set_names(fk_key_col_in_fk_table_name) |>
    # Join with the fk parent table
    dplyr::left_join(this_fk_parent_table, by = fk_key_col_in_fk_table_name) |>
    # Extract the column that we want to return
    magrittr::extract2(fk_value_col_in_fk_table_name)
  # Check for errors
  errs <- which(is.na(out), arr.ind = TRUE)
  if (length(errs) != 0) {
    bad_v_key <- v_key[errs]

    err_msg <- paste0("Unknown fk keys in decode_fk_values():\n",
                      paste0(bad_v_key, collapse = ", "))
    stop(err_msg)
  }
  return(out)
}


#' Encode and decode `matsindf` data frames for storage in a database
#'
#' The CL-PFU database enables storage of `matsindf` data frames
#' by encoding matrix values in triplet format.
#' These functions perform encoding and decoding
#' of `matsindf` data frames.
#' [encode_matsindf()] and [decode_matsindf()] are inverses of each other.
#'
#' `index_map` must be
#' an unnamed list of two data frames or
#' a named list of two or more data frames.
#' * If an unnamed list of exactly two data frames,
#'   each data frame must have only
#'   an integer column and a character column.
#'   The first data frame of `index_map`
#'   is interpreted as the mapping
#'   between row names and row indices
#'   and
#'   the second data frame of `index_map`
#'   is interpreted as the mapping
#'   between column names and column indices.
#' * If a named list of two or more data frames,
#'   the names of `index_map`
#'   are interpreted as row and column types,
#'   with named data frames applied as the mapping for the
#'   associated row or column type.
#'   For example the data frame named "Industry" would be applied
#'   to the dimension (row or column)
#'   with an "Industry" type.
#'   When both row and column have "Industry" type,
#'   the "Industry" mapping is applied to both.
#'   When sending named data frames in `index_map`,
#'   matrices to be encoded must have both a row type and a column type.
#'   If an appropriate mapping cannot be found in `index_map`,
#'   an error is raised.
#'   Both matching data frames must have only
#'   an integer column and
#'   a character column.
#'
#' `.matsindf` can be
#' (a) wide by matrices,
#' with matrix names as column names or
#' (b) tidy, with `matname` and `matval` columns.
#'
#' If `.matsindf` does not contain any matrix columns,
#' `.matsindf` is returned unchanged.
#'
#' If `.encoded` does not contain a `matname` column,
#' `.encoded` is returned unchanged.
#'
#' By default, [encode_matsindf()] will return
#' zero-row data frames when
#' encoding zero matrices.
#' Set `retain_zero_structure = TRUE`
#' to return all entries in zero matrices.
#'
#' All of `matname`, `row_index_colname`,
#' `col_index_colname`, and `val_colname`
#' must be present in `.encoded`.
#' If not, `.encoded` is returned unmodified.
#'
#' @param .matsindf A matsindf data frame whose matrices are to be encoded.
#' @param .encoded A data frame of matrices in triplet form whose matrices are to be decoded.
#' @param index_map A list of two or more index map data frames.
#'                  Default is `list(Industry = industry_index_map, Product = product_index_map)`.
#' @param rctypes A data frame of row and column types.
#' @param wide_by_matrices A boolean that tells whether to
#'                         [tidyr::pivot_wider()] the results.
#'                         Default is `TRUE`.
#' @param industry_index_map,product_index_map Optional data frames with two columns providing the mapping
#'                                             between row and column indices and row and column names.
#'                                             See details.
#' @param matrix_class The class of matrices to be created by [decode_matsindf()].
#'                     One of "matrix" (the default
#'                     and `R`'s native matrix class) or
#'                     "Matrix" (for sparse matrices).
#' @param retain_zero_structure A boolean that tells whether to retain
#'                              the structure of zero matrices when creating triplets.
#'                              Default is `FALSE`.
#'                              See details.
#' @param matname The name of the column in `.matsindf` that contains matrix names.
#'                 Default is "matname".
#' @param matval The name of the column in `.matsindf` that contains matrix values.
#'                Default is "matval".
#' @param row_index_colname The name of the row index column in `.encoded`.
#'                          Default is "i".
#' @param col_index_colname The name of the column index column in `.encoded`.
#'                          Default is "j".
#' @param rowtype_colname,coltype_colname Names of `rowtype` and `coltype` columns.
#' @param value_colname The name of the value column.
#'                      Default is "value".
#'
#' @return For [encode_matsindf()],
#'         a version of `.matsindf` with matrices in triplet form,
#'         appropriate for insertion into a database.
#'         For [decode_matsindf()],
#'         a version of `.encoded` appropriate for in-memory
#'         analysis and calculations.
#'
#' @name encode_decode_matsindf


#' @rdname encode_decode_matsindf
#' @export
decode_matsindf <- function(.encoded,
                            index_map,
                            rctypes,
                            wide_by_matrices = TRUE,
                            matrix_class = c("matrix", "Matrix"),
                            matname = PFUPipelineTools::mat_meta_cols$matname,
                            matval = PFUPipelineTools::mat_meta_cols$matval,
                            row_index_colname = PFUPipelineTools::mat_colnames$i,
                            col_index_colname = PFUPipelineTools::mat_colnames$j,
                            value_colname = PFUPipelineTools::mat_colnames$value,
                            rowtype_colname = PFUPipelineTools::mat_meta_cols$rowtype,
                            coltype_colname = PFUPipelineTools::mat_meta_cols$coltype) {

  if (!all(c(matname, row_index_colname, col_index_colname, value_colname)
           %in% colnames(.encoded))) {
    return(.encoded)
  }
  matrix_class <- match.arg(matrix_class)
  out <- .encoded |>
    matsindf::group_by_everything_except(row_index_colname, col_index_colname, value_colname) |>
    tidyr::nest(.key = matval) |>
    dplyr::ungroup() |>
    dplyr::left_join(rctypes, by = matname, copy = TRUE) |>
    dplyr::mutate(
      # Need to set row and column type differently,
      # because setrowtype and setcoltype will apply rowtype and coltype
      # to each column of the data frame.
      "{matval}" := Map(f = matsbyname::setrowtype, a = .data[[matval]], rowtype = .data[[rowtype_colname]]),
      "{matval}" := Map(f = matsbyname::setcoltype, a = .data[[matval]], coltype = .data[[coltype_colname]]),
      "{rowtype_colname}" := NULL,
      "{coltype_colname}" := NULL,
      "{matval}" := .data[[matval]] |>
        matsbyname::to_named_matrix(index_map = index_map,
                                    matrix_class = matrix_class,
                                    row_index_colname = row_index_colname,
                                    col_index_colname = col_index_colname,
                                    value_colname = value_colname)
    )
  if (wide_by_matrices) {
    out <- out |>
      tidyr::pivot_wider(names_from = matname,
                         values_from = matval)
  }
  return(out)
}


#' @rdname encode_decode_matsindf
#' @export
encode_matsindf <- function(.matsindf,
                            index_map = list(industry_index_map,
                                             product_index_map) |>
                              magrittr::set_names(c(IEATools::row_col_types$industry,
                                                    IEATools::row_col_types$product)),
                            industry_index_map,
                            product_index_map,
                            retain_zero_structure = FALSE,
                            matname = PFUPipelineTools::mat_meta_cols$matname,
                            matval = PFUPipelineTools::mat_meta_cols$matval,
                            row_index_colname = PFUPipelineTools::mat_colnames$i,
                            col_index_colname = PFUPipelineTools::mat_colnames$j,
                            value_colname = PFUPipelineTools::mat_colnames$value) {

  # Find matrix column names
  matcols <- matsindf::matrix_cols(.matsindf, .any = TRUE) |>
    names()
  if (length(matcols) == 0) {
    # These aren't the droids you are looking for.
    return(.matsindf)
  }
  if (!(matval %in% colnames(.matsindf))) {
    # We have at least 1 column of matrices
    # but not the matval column.
    # Pivot to a tidy data frame.
    .matsindf <- .matsindf |>
      tidyr::pivot_longer(cols = dplyr::all_of(matcols),
                          names_to = matname,
                          values_to = matval)
  }
  # Ensure that both matname and matval are present
  assertthat::assert_that(matname %in% colnames(.matsindf),
                          msg = paste0("Matrix name column '", matname, "' missing from .matsindf in encode_matsindf()"))
  assertthat::assert_that(matval %in% colnames(.matsindf),
                          msg = paste0("Matrix value column '", matval, "' missing from .matsindf in encode_matsindf()"))

  .matsindf |>
    dplyr::mutate(
      # Convert the matval column triplet form
      "{matval}" := matsbyname::to_triplet(.data[[matval]],
                                           index_map,
                                           retain_zero_structure = retain_zero_structure,
                                           row_index_colname = row_index_colname,
                                           col_index_colname = col_index_colname,
                                           value_colname = value_colname)
    ) |>
    tidyr::unnest(cols = dplyr::all_of(matval))
}


#' Filter a database table based on a version string.
#'
#' Filtering a database table based on the version you wish to download
#' is a common task.
#' In the CL-PFU database, we store data in a compressed format where
#' identical data points are not duplicated.
#' Rather, they are stored in a single row with the `ValidFromVersion`
#' and `ValidToVersion` columns incremented appropriately.
#'
#' The desired version is supplied in the `version_string` argument,
#' which can be a vector of any length.
#'
#' If both `tbl` and `db_table_name` are provided, `db_table_name` is
#' ignored.
#'
#' @param tbl The `tbl` object that should be filtered.
#' @param version_string A vector of version strings to indicate the desired version(s).
#' @param collect A boolean that tells whether to collect `tbl` from `conn`
#'                before returning.
#'                Default is `FALSE`.
#' @param db_table_name The name of the table to be filtered.
#' @param conn An optional database connection.
#'             Necessary only for the default values of `schema` and `fk_parent_tables`.
#'             Default is `NULL`.
#' @param schema The database schema (a `dm` object).
#'               Default calls `schema_from_conn()`, but
#'               you can supply a pre-computed schema for speed.
#'               Needed only when `decode_fks = TRUE` (the default).
#'               If foreign keys are not being decoded,
#'               setting `NULL` may improve speed.
#' @param fk_parent_tables Foreign key parent tables to assist decoding
#'                         foreign keys.
#'                         Default calls `get_all_fk_tables()`.
#' @param valid_from_version_colname The name of the ValidFromVersion column.
#'                                   Default is
#'                                   `PFUPipelineTools::version_cols$valid_from_version`.
#' @param valid_to_version_colname The name of the ValidToVersion column.
#'                                 Default is
#'                                 `PFUPipelineTools::version_cols$valid_to_version`.
#'
#' @returns A filtered version of `tbl`.
#'
#' @export
filter_on_version_string <- function(tbl,
                                     version_string,
                                     db_table_name,
                                     collect = FALSE,
                                     conn = NULL,
                                     schema = schema_from_conn(conn = conn),
                                     fk_parent_tables = get_all_fk_tables(conn = conn, schema = schema),
                                     valid_from_version_colname = PFUPipelineTools::version_cols$valid_from_version,
                                     valid_to_version_colname = PFUPipelineTools::version_cols$valid_to_version) {

  version_string <- unique(version_string)

  if (length(version_string) > 1) {
    # Eliminate duplicates
    out_list <- lapply(unique(version_string), function(this_version_string) {
      # If we have more than one version_string,
      # call ourselves recursively and stack the results.
      filter_on_version_string(tbl = tbl,
                               version_string = this_version_string,
                               db_table_name = db_table_name,
                               collect = collect,
                               conn = conn,
                               schema = schema,
                               fk_parent_tables = fk_parent_tables,
                               valid_from_version_colname = valid_from_version_colname,
                               valid_to_version_colname = valid_to_version_colname)
    })
    # I would like to say
    # out <- dplyr::rbind(out)
    # but that doesn't work with objects that are not data frames.
    # So, instead, use union_all(), which successfully stacks tbl objects.
    out <- out_list[[1]]
    for (i in 2:length(out_list)) {
      out <- out |>
        dplyr::union_all(out_list[[i]])
    }
    return(out)
  }

  # Make sure we have only one version_string.
  assertthat::assert_that(length(version_string) == 1,
                          msg = paste("version_string must be length 1 in",
                                      "PFUPipelineTools::filter_on_version_string()"))

  # Figure out the index for the version of interest to us.
  tryCatch(
    version_index <- encode_version_string(version_string = version_string,
                                           table_name = db_table_name,
                                           schema = schema,
                                           fk_parent_tables = fk_parent_tables),
    error = function(e) {
      stop(paste0('Unknown version_string "', version_string, '".'))
    }
  )

  # Filter the outgoing data frame according to the version_index
  out <- tbl |>
    dplyr::filter(.data[[valid_from_version_colname]] <= version_index) |>
    dplyr::filter(.data[[valid_to_version_colname]] >= version_index)

  if (collect) {
    out <- out |>
      dplyr::collect()
  }

  return(out)
}


#' Encode a version string
#'
#' This function encodes a version string,
#' returning the integer representation of the version string.
#'
#' @param version_string The version to be encoded.
#' @param table_name The name of the table in which the `version_string` is found.
#' @param conn An optional database connection.
#'             Necessary only for the default values of `schema` and `fk_parent_tables`.
#'             Default is `NULL`.
#' @param schema The schema for the database.
#'               Default is `schema_from_conn(conn = conn)`.
#' @param fk_parent_tables The foreign key parent tables.
#'                         Default is `get_all_fk_tables(conn = conn, schema = schema)`.
#' @param .version_colname A column in which versions are provided.
#'                         This column is used internally.
#'                         Default is `PFUPipelineTools::version_cols$valid_from_version`.
#'                         `PFUPipelineTools::version_cols$valid_to_version`
#'                         would also work.
#'
#' @export
#'
#' @return An integer which is the index for `version_string`.
encode_version_string <- function(version_string,
                                  table_name,
                                  conn = NULL,
                                  schema = schema_from_conn(conn = conn),
                                  fk_parent_tables = get_all_fk_tables(conn = conn, schema = schema),
                                  .version_colname = PFUPipelineTools::version_cols$valid_from_version) {
  # Make sure we have only 1 version
  assertthat::assert_that(length(version_string) == 1,
                          msg = paste("version_string must have length 1",
                                      "in PFUPipelineTools:::encode_version_string()."))
  # Make a small data frame that will be used to decode the version.
  mini_df <- tibble::tribble(~c1,
                             version_string) |>
    magrittr::set_names(.version_colname)
  # Encode the versions according to foreign keys
  version_index <- mini_df |>
    encode_fks(db_table_name = table_name,
               schema = schema,
               fk_parent_tables = fk_parent_tables) |>
    # Pull out the only integer we find
    magrittr::extract2(.version_colname)
  # Do some error checking
  assertthat::assert_that(length(version_index) != 0,
                          msg = paste("Didn't find a version that matches", version,
                                      "in PFUPipelineTools::pl_collect_from_hash()."))
  return(version_index)
}







