#' Read a CL-PFU database schema file
#'
#' The SchemaAndSimpleTables.xlsx file contains a sheet that
#' represents the schema for the CL-PFU database.
#' The sheet is designed to enable easy changes to the CL-PFU database schema
#' for successive versions of the database.
#' This function reads the schema table from the SchemaAndSimpleTables.xlsx file.
#'
#' @param version The database version for input information.
#' @param schema_path The path to the schema file.
#'                    Default is `PFUSetup::get_abs_paths()[["schema_path"]]`
#' @param schema_sheet The name of the sheet in the schema file at `schema_path` that contains
#'                     information about tables, columns, and primary and foreign keys.
#'                     Default is "Schema".
#'
#' @return A `dm` object containing schema information for the CL-PFU database.
#'
#' @export
#'
#' @examples
#' \dontrun{
#' load_schema_table(version = "v2.0")
#' }
load_schema_table <- function(version,
                              schema_path = PFUSetup::get_abs_paths(version = version)[["schema_path"]],
                              schema_sheet = "Schema") {
  schema_path |>
    readxl::read_excel(sheet = schema_sheet)
}


#' Load foreign key tables for the CL-PFU database from a spreadsheet
#'
#' The SchemaAndSimpleTables.xlsx file contains a sheet that
#' represents the schema for the CL-PFU database.
#' The sheet is designed to enable easy changes to the CL-PFU database schema
#' for successive versions of the database.
#' All other tabs (besides `readme_sheet`) are foreign key tables.
#' This function reads all foreign key tables from the SchemaAndSimpleTables.xlsx file
#' and returns a named list of those tables, each in data frame format.
#'
#' `readme_sheet` and `schema_sheet` are ignored.
#' All other sheets in the file at `schema_path` are assumed to be
#' foreign key tables that are meant to be uploaded to the database.
#'
#' @param version The database version for input information.
#' @param simple_tables_path The path to the file containing simple tables.
#'                           Default is `PFUSetup::get_abs_paths()[["schema_path"]]`
#' @param readme_sheet The name of the sheet in the file at `simple_tables_path`
#'                     that contains readme information.
#'                     Default is "README".
#' @param schema_sheet The name of the sheet in the in the file at `simple_tables_path`
#'                     that contains schema information.`
#'                     Default is "Schema".
#' @param .table,.colname,.coldatatype See `PFUPipelineTools::schema_table_colnames`.
#'
#' @return A named list of data frames, each containing a foreign key table
#'         for the CL-PFU database.
#'
#' @export
load_fk_tables <- function(version,
                           simple_tables_path = PFUSetup::get_abs_paths(version = version)[["schema_path"]],
                           readme_sheet = "README",
                           schema_sheet = "Schema",
                           .table = PFUPipelineTools::schema_table_colnames$table,
                           .colname = PFUPipelineTools::schema_table_colnames$colname,
                           .coldatatype = PFUPipelineTools::schema_table_colnames$coldatatype) {
  # Read schema_table for data types
  schema_table <- simple_tables_path |>
    readxl::read_excel(sheet = schema_sheet)

  simple_tables_path |>
    readxl::excel_sheets() |>
    setdiff(c(readme_sheet, schema_sheet)) |>
    self_name() |>
    lapply(FUN = function(this_sheet_name) {
      # Get the data types for this simple table
      dtypes <- schema_table |>
        dplyr::filter(.data[[.table]] == this_sheet_name) |>
        dplyr::select(dplyr::all_of(c(.colname, .coldatatype)))

      # Get the table
      this_table <- simple_tables_path |>
        readxl::read_excel(sheet = this_sheet_name)

      # Cycle through each column in the table and set its data type
      for (this_colname in colnames(this_table)) {
        # Get the data type
        this_data_type <- dtypes |>
          dplyr::filter(.data[[.colname]] == this_colname) |>
          dplyr::select(dplyr::all_of(.coldatatype)) |>
          unlist() |>
          unname()
        if (this_data_type == "int") {
          this_table[[this_colname]] <- as.integer(this_table[[this_colname]])
        } else if (this_data_type == "text") {
          this_table[[this_colname]] <- as.character(this_table[[this_colname]])
        } else if (this_data_type == "boolean") {
          this_table[[this_colname]] <- as.logical(this_table[[this_colname]])
        } else if (this_data_type == "double precision") {
          this_table[[this_colname]] <- as.double(this_table[[this_colname]])
        } else {
          stop(paste0("Unknown data type: '", this_data_type, "' in load_fk_tables()"))
        }
      }
      return(this_table)
    })
}


#' Create a data model from an Excel schema table
#'
#' This function returns a `dm` object suitable for
#' future uploading to a database.
#'
#' `schema_table` is assumed to be a data frame with the following columns:
#'
#'   - `.table`: gives table names in the database.
#'   - `.colname`: gives column names in each table; if suffixed with `pk_suffix`,
#'                 interpreted as a primary key column.
#'   - `.is_pk`: tells if `.colname` is a primary key for `.table`.
#'   - `.coldatatype`: gives the data type for the column,
#'                     one of "int", "boolean", "text", or "double precision".
#'   - `.fk_table`: gives a table in which the foreign key can be found
#'   - `.fk_colname`: gives the column name in fk.table where the foreign key can be found
#'
#' @param schema_table A schema table, typically the output of `load_schema_table()`.
#' @param pk_suffix The suffix for primary keys.
#'                  Default is "_ID".
#' @param .table,.colname,.is_pk,.coldatatype,.fk_table,.fk_colname See `PFUPipelineTools::schema_table_colnames`.
#' @param .pk_cols Column names used internally.
#'
#' @return A `dm` object created from `schema_table`.
#'
#' @export
#'
#' @examples
#' st <- tibble::tribble(~Table, ~Colname, ~IsPK, ~ColDataType, ~FKTable, ~FKColname,
#'                       "Country", "CountryID", TRUE, "text", "NA", "NA",
#'                       "Country", "Country", FALSE, "text", "NA", "NA",
#'                       "Country", "Description", FALSE, "text", "NA", "NA")
#' schema_dm(st)
schema_dm <- function(schema_table,
                      pk_suffix = PFUPipelineTools::key_col_info$pk_suffix,
                      .table = PFUPipelineTools::schema_table_colnames$table,
                      .colname = PFUPipelineTools::schema_table_colnames$colname,
                      .is_pk = PFUPipelineTools::schema_table_colnames$is_pk,
                      .coldatatype = PFUPipelineTools::schema_table_colnames$coldatatype,
                      .fk_table = PFUPipelineTools::schema_table_colnames$fk_table,
                      .fk_colname = PFUPipelineTools::schema_table_colnames$fk_colname,
                      .pk_cols = ".pk_cols") {

  dm_tables <- schema_table[[.table]] |>
    unique() |>
    self_name() |>
    lapply(FUN = function(this_table_name) {
      colnames <- schema_table |>
        dplyr::filter(.data[[.table]] == this_table_name) |>
        magrittr::extract2(.colname)
      # Convert to a data frame
      this_mat <- matrix(nrow = 0, ncol = length(colnames), dimnames = list(NULL, colnames))
      this_dm_table <- this_mat |>
        as.data.frame() |>
        tibble::as_tibble()
      # Set data types
      coldatatypes <- schema_table |>
        dplyr::filter(.data[[.table]] == this_table_name) |>
        magrittr::extract2(.coldatatype)
      for (icol in 1:length(coldatatypes)) {
        this_data_type <- coldatatypes[[icol]]
        if (this_data_type == "int") {
          this_dm_table[[icol]] <- as.integer(this_dm_table[[icol]])
        } else if (this_data_type == "text") {
          this_dm_table[[icol]] <- as.character(this_dm_table[[icol]])
        } else if (this_data_type == "boolean") {
          this_dm_table[[icol]] <- as.logical(this_dm_table[[icol]])
        } else if (this_data_type == "double precision") {
          this_dm_table[[icol]] <- as.double(this_dm_table[[icol]])
        } else {
          stop(paste0("Unknown data type: '", this_data_type, "' in schema_dm()"))
        }
      }
      return(this_dm_table)
    }) |>
    dm::as_dm()

  # Set primary key according to the the IsPK column
  # in schema_table.
  # Note, there could be multiple primary keys for a table.
  # Get the primary key info for all tables.
  pk_info <- schema_table |>
    dplyr::filter(.data[[.is_pk]]) |>
    dplyr::select(dplyr::all_of(c(.table, .colname))) |>
    # dplyr::rename(.pk_cols = .data[[.colname]])
    dplyr::rename(.pk_cols = dplyr::all_of(.colname))
  # Get a list of all tables in the schema
  tables <- schema_table |>
    dplyr::select(dplyr::all_of(.table)) |>
    unlist() |>
    unname() |>
    unique()
  # Cycle through each table, setting primary keys
  # in the data model (dm_tables)
  for (this_table in tables) {
    these_pk_cols <- pk_info |>
      dplyr::filter(.data[[.table]] == this_table) |>
      dplyr::select(dplyr::all_of(.pk_cols)) |>
      unlist() |>
      unname()
    dm_tables <- dm_tables |>
      dm::dm_add_pk(table = {{this_table}},
                    columns = {{these_pk_cols}})
  }

  # Set foreign keys according to the schema_table
  fk_info <- schema_table |>
    dplyr::filter(.data[[.fk_colname]] != "NA")

  if (nrow(fk_info) > 0) {
    for (irow in 1:nrow(fk_info)) {
      this_table_name <- fk_info[[.table]][[irow]]
      colname <- fk_info[[.colname]][[irow]]
      fk_table <- fk_info[[.fk_table]][[irow]]
      fk_colname <- fk_info[[.fk_colname]][[irow]]
      dm_tables <- dm_tables |>
        dm::dm_add_fk(table = {{this_table_name}},
                      columns = {{colname}},
                      ref_table = {{fk_table}},
                      ref_columns = {{fk_colname}},
                      check = TRUE)
    }
  }
  return(dm_tables)
}


#' Upload a schema and simple tables for the CL-PFU database
#'
#' When you need to start over again,
#' you need to delete database tables, re-define the schema, and upload simple tables.
#' This function makes it easy.
#'
#' Optionally (by setting `drop_db_tables = TRUE`),
#' deletes existing tables in the database before uploading
#' the schema (`schema`) and simple tables (`simple_tables`).
#' `drop_db_tables` is `FALSE` by default.
#' However, it is unlikely that this function will succeed unless
#' `drop_db_tables` is set `TRUE`, because
#' uploading the data model `schema` to `conn` will fail
#' if the tables already exist in the database at `conn`.
#'
#' `simple_tables` should not include tables with foreign keys,
#' because the order for uploading `simple_tables` is not guaranteed.
#' Doing so will avoid uploading a table with a foreign key before
#' the parent table containing the foreign key values has been uploaded.
#'
#' `set_not_null_constraints` controls setting of
#' `NOT NULL` constraints for all foreign key columns in `schema`.
#'
#' `conn`'s user must have superuser privileges.
#'
#' @param schema A data model (a `dm` object).
#' @param simple_tables A named list of data frames with the content of
#'                      tables in `conn`.
#' @param conn A `DBI` connection to a database.
#' @param set_not_null_constraints A boolean that tells whether
#'                                 `NOT NULL` constraints are set on
#'                                 foreign key columns in `schema`.
#'                                 Default is `TRUE`.
#' @param drop_db_tables If `TRUE`, all tables in conn are dropped.
#'                       If a character vector, the names of tables to be dropped.
#'                       If `FALSE` (the default), no tables are dropped.
#'                       existing tables before uploading the new schema.
#' @param .pk_col See `PFUPipelineTools::dm_pk_colnames`.
#'
#' @return The remote data model
#'
#' @export
pl_upload_schema_and_simple_tables <- function(schema,
                                               simple_tables,
                                               conn,
                                               set_not_null_constraints = TRUE,
                                               drop_db_tables = FALSE,
                                               .pk_col = PFUPipelineTools::dm_pk_colnames$pk_col) {
  # Get rid of the tables, but not the targets cache, if desired
  pl_destroy(conn, destroy_cache = FALSE, drop_tables = drop_db_tables)
  # Copy the data model to conn
  dm::copy_dm_to(dest = conn, dm = schema, temporary = FALSE)

  if (set_not_null_constraints) {
    # Set NOT NULL constraints on all foreign key columns
    set_not_null_constraints_on_fk_cols(schema, conn)
  }

  # Upload the simple tables
  for (this_table_name in names(simple_tables)) {
    # Get the primary keys data frame
    pk_table <- dm::dm_get_all_pks(schema, table = {{this_table_name}})
    # Make sure we have one and only one primary key column
    assertthat::assert_that(nrow(pk_table) == 1,
                            msg = paste0("Table '",
                                         this_table_name,
                                         "' has ", nrow(pk_table),
                                         " primary keys. 1 is required."))
    # Get the primary key name as a string
    pk_str <- pk_table |>
      # "pk_col" is the name of the column of primary key names
      # in the tibble returned by dm::dm_get_all_pks()
      magrittr::extract2(.pk_col) |>
      magrittr::extract2(1)

    # Upload the simple table to conn
    dplyr::tbl(conn, this_table_name) |>
      dplyr::rows_upsert(simple_tables[[this_table_name]],
                         by = pk_str,
                         copy = TRUE,
                         in_place = TRUE)
  }
}


#' Set NOT NULL constraints on foreign key columns
#'
#' To maintain the integrity of data in a database,
#' it is important to enforce NOT NULL constraints on foreign key columns.
#' This function adds NOT NULL constraints to every foreign key column
#' in `schema`.
#'
#' @param schema A `dm` object describing the schema for the database at `conn`.
#' @param conn A database connection.
#' @param .child_table,.child_fk_cols See `PFUPpipelineTools::dm_fk_colnames`.
#'
#' @return `NULL` silently.
#'
#' @export
set_not_null_constraints_on_fk_cols <- function(schema,
                                                conn,
                                                .child_table = PFUPipelineTools::dm_fk_colnames$child_table,
                                                .child_fk_cols = PFUPipelineTools::dm_fk_colnames$child_fk_cols) {
  # Find details about foreign keys
  fk_details <- schema |>
    dm::dm_get_all_fks() |>
    dplyr::select(dplyr::all_of(c(.child_table, .child_fk_cols))) |>
    dplyr::mutate(
      # Convert <keys> to <chr>
      "{.child_fk_cols}" := unlist(.data[[.child_fk_cols]])
    )

  # If there are no foreign keys, just return.
  if (nrow(fk_details) == 0) {
    return(invisible(NULL))
  }

  # Loop over all of the foreign keys
  # and set the NOT NULL constraint.
  for (i in 1:nrow(fk_details)) {
    this_child_table <- fk_details[[.child_table]][[i]]
    this_child_fk_col <- fk_details[[.child_fk_cols]][[i]]
    stmt <- paste0('ALTER TABLE "', this_child_table,
                   '" ALTER COLUMN "', this_child_fk_col,
                   '" set NOT NULL;')
    DBI::dbExecute(conn, stmt)
  }
  return(invisible(NULL))
}


#' Upsert a data frame with optional recoding of foreign keys
#'
#' Upserts
#' (inserts or updates,
#' depending on whether the private keys in `.df`
#' already exist in `db_table_name`)
#' `.df` into `db_table_name` at `conn`.
#'
#' This function decodes foreign keys (fks), when possible,
#' assuming that all fks are integers.
#' If non-integers (typically, character strings)
#' are provided in fk columns of `.df`,
#' the non-integers will be recoded to their appropriate integer key values.
#'
#' This function knows about CL-PFU database tables that contain
#' matrix information.
#' In particular, if `.df` contains matrices,
#' they are expanded into row-col-val format
#' before uploading.
#'
#' The output of this function is a special data frame that
#' contains the following columns:
#'
#'   * All single-valued columns columns in `.df` and
#'     columns given in `additional_hash_group_cols`
#'     (default `PFUPipelineTools::additional_hash_group_cols`).
#'   * Hash: A column with a hash of all non-foreign-key columns.
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
#' The user in `conn` must have write access to the database.
#'
#' @param .df The data frame to be upserted.
#' @param conn A connection to the CL-PFU database.
#' @param db_table_name A string identifying the destination for `.df` in `conn`,
#'                      i.e. the name of a remote database table.
#'                      Default is `NULL`, meaning that the value for this argument
#'                      will be taken from the `.db_table_name` column of `.df`.
#' @param additional_hash_group_cols A vector or list of additional columns
#'                                   by which `.df` will be grouped
#'                                   before hashing.
#'                                   Default is `PFUPipelineTools::additional_hash_group_cols`.
#'                                   Set to `NULL` to group by all columns
#'                                   with only 1 unique value.
#' @param in_place A boolean that tells whether to modify the database at `conn`.
#'                 Default is `FALSE`, which is helpful if you want to chain
#'                 several requests.
#' @param encode_fks A boolean that tells whether to code foreign keys in `.df`.
#'                   Default is `TRUE`.
#' @param schema The data model (`dm` object) for the database in `conn`.
#'               Default is `dm_from_con(conn, learn_keys = TRUE)`.
#'               See details.
#' @param fk_parent_tables A named list of all parent tables
#'                         for the foreign keys in `db_table_name`.
#'                         See details.
#' @param .db_table_name The name of the table name column in `.df`.
#'                       Default is `PFUPipelineTools::hashed_table_colnames$db_table_name`.
#' @param .pk_col The name of the primary key column in a primary key table.
#'                See `PFUPipelineTools::dm_pk_colnames`.
#' @param .algo The hashing algorithm.
#'              Default is "md5".
#'
#' @return A hash of `.df` according to `.algo`.
#'
#' @seealso `pl_download()` for the reverse operation.
#'          `pl_upload_schema_and_simple_tables()` for a way to establish the database schema.
#'
#' @export
pl_upsert <- function(.df,
                      conn,
                      db_table_name = NULL,
                      additional_hash_group_cols = PFUPipelineTools::additional_hash_group_cols,
                      in_place = FALSE,
                      encode_fks = TRUE,
                      schema = schema_from_conn(conn),
                      fk_parent_tables = get_all_fk_tables(conn = conn,
                                                           schema = schema,
                                                           collect = TRUE),
                      .db_table_name = PFUPipelineTools::hashed_table_colnames$db_table_name,
                      .pk_col = PFUPipelineTools::dm_pk_colnames$pk_col,
                      .algo = "md5") {

  if (is.null(db_table_name)) {
    db_table_name <- .df[[.db_table_name]] |>
      unique()
  }
  if (length(db_table_name) != 1) {
    stop("length(db_table_name) must be 1 in pl_upsert()")
  }

  # Eliminate the .db_table_name column if it exists.
  # We don't upload the table with that column.
  .df <- .df |>
    dplyr::mutate(
      "{.db_table_name}" := NULL
    )

  pk_table <- dm::dm_get_all_pks(schema, table = dplyr::all_of({{db_table_name}}))
  # Make sure we have one and only one primary key row
  assertthat::assert_that(nrow(pk_table) == 1,
                          msg = paste0("Table '",
                                       db_table_name,
                                       "' has ", nrow(pk_table),
                                       " primary keys. 1 is required."))
  # Get the primary key name as a string for later use in the upsert command
  pk_str <- pk_table |>
    # .pk_col is the name of the column of primary key names
    # in the tibble returned by dm::dm_get_all_pks()
    magrittr::extract2(.pk_col) |>
    magrittr::extract2(1)

  # Encode fk column values in .df with integer keys, if requested.
  if (encode_fks) {
    .df <- .df |>
      encode_fks(db_table_name = db_table_name,
                 schema = schema,
                 fk_parent_tables = fk_parent_tables)
  }

  dplyr::tbl(conn, db_table_name) |>
    dplyr::rows_upsert(.df,
                       by = pk_str,
                       copy = TRUE,
                       in_place = in_place)
  # Return a hash of .df
  .df |>
    pl_hash(table_name = db_table_name,
            additional_hash_group_cols = additional_hash_group_cols)
}


#' Encode foreign keys in a data frame to be uploaded
#'
#' In the CL-PFU pipeline,
#' we allow data frames about to be uploaded
#' to the database
#' to have foreign key values (usually strings)
#' instead of foreign keys (integers)
#' in foreign key columns.
#' This function translates the fk values
#' to fk keys.
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
#' If `.df` contains no foreign key columns,
#' `.df` is returned unmodified.
#'
#' @param .df The data frame about to be uploaded.
#' @param db_table_name The string name of the database table where `.df` is to be uploaded.
#' @param conn An optional database connection.
#'             Necessary only for the default values of `schema` and `fk_parent_tables`.
#' @param schema The data model (`dm` object) for the database in `conn`.
#'               See details.
#' @param fk_parent_tables A named list of all parent tables
#'                         for the foreign keys in `db_table_name`.
#'                         See details.
#' @param .child_table,.child_fk_cols,.parent_table,.parent_key_cols See `PFUPipelineTools::dm_fk_colnames`.
#' @param .pk_suffix See `PFUPipelineTools::key_col_info`.
#'
#' @seealso [decode_fks()] for the inverse operation,
#'          albeit for all keys, primary and foreign.
#'
#' @return A version of `.df` with fk values (often strings)
#'         replaced by fk keys (integers).
#'
#' @export
encode_fks <- function(.df,
                       db_table_name,
                       conn,
                       schema = schema_from_conn(conn),
                       fk_parent_tables = get_all_fk_tables(conn = conn, schema = schema),
                       .child_table = PFUPipelineTools::dm_fk_colnames$child_table,
                       .child_fk_cols = PFUPipelineTools::dm_fk_colnames$child_fk_cols,
                       .parent_table = PFUPipelineTools::dm_fk_colnames$parent_table,
                       .parent_key_cols = PFUPipelineTools::dm_fk_colnames$parent_key_cols,
                       .pk_suffix = PFUPipelineTools::key_col_info$pk_suffix) {

  # Get details of all foreign keys in .df
  fk_details_for_db_table <- schema |>
    dm::dm_get_all_fks() |>
    dplyr::filter(.data[[.child_table]] == db_table_name) |>
    dplyr::mutate(
      # Remove the <keys> class on child_fk_cols and parent_key_cols.
      "{.child_fk_cols}" := unlist(.data[[.child_fk_cols]]),
      "{.parent_key_cols}" := unlist(.data[[.parent_key_cols]])
    )

  if (nrow(fk_details_for_db_table) == 0) {
    # There are no foreign key columns in .df,
    # so just return .df unmodified.
    return(.df)
  }

  # Get a vector of string names of foreign key columns in .df
  fk_cols_in_df <- fk_details_for_db_table |>
    dplyr::select(dplyr::all_of(.child_fk_cols)) |>
    unlist() |>
    unname()

  encoded_df <- .df

  for (this_fk_col_in_df in fk_cols_in_df) {
    if (is.integer(.df[[this_fk_col_in_df]])) {
      # This is already an integer, and
      # presumably no need to recode
      next
    }
    # Get the parent table name for this column
    parent_table_name <- fk_details_for_db_table |>
      dplyr::filter(.data[[.child_fk_cols]] == this_fk_col_in_df) |>
      dplyr::select(dplyr::all_of(.parent_table)) |>
      unlist() |>
      unname()
    # Get the parent table's column
    parent_table_fk_colname <- fk_details_for_db_table |>
      dplyr::filter(.data[[.child_fk_cols]] == this_fk_col_in_df) |>
      dplyr::select(dplyr::all_of(.parent_key_cols)) |>
      unlist() |>
      unname()
    parent_table_fk_value_colname <- gsub(pattern = paste0(.pk_suffix, "$"),
                                          replacement = "",
                                          x = parent_table_fk_colname)
    encoded_df <- encoded_df |>
      dplyr::left_join(this_fk_col_parent_table,
                       by = dplyr::join_by({{this_fk_col_in_df}} == {{parent_table_fk_value_colname}})) |>
      dplyr::mutate(
        "{this_fk_col_in_df}" := NULL
      ) |>
      dplyr::rename(
        "{this_fk_col_in_df}" := dplyr::all_of(parent_table_fk_colname)
      )
    # Check for errors and provide a nice message if there is a problem.
    if (any(is.na(encoded_df[[this_fk_col_in_df]]))) {
      # One of the levels didn't match.
      # Catch this error here to give an intelligent error message.
      # First create a list of strings we could not encode.
      cant_encode <- which(is.na(encoded_df[[this_fk_col_in_df]]))
      unmatched_names <- paste0("'", .df[[this_fk_col_in_df]][cant_encode], "'") |>
        unique() |>
        paste0(collapse = "\n")
      err_msg <- paste0("In PFUPipelineTools::encode_fks(),\n",
                        "unable to convert the following entries to foreign keys\n",
                        "in the '",
                        this_fk_col_in_df, "' column ",
                        "of the '",
                        db_table_name,
                        "' table:\n",
                        unmatched_names,
                        "\nIs something misspelled?\n",
                        "Or should they be added to the\n'",
                        parent_table_fk_colname,
                        "' and '",
                        parent_table_fk_value_colname,
                        "' columns\n",
                        "of the '",
                        parent_table_name,
                        "' table?")
      stop(err_msg)
    }
  }
  return(encoded_df)
}


#' Decode keys in a database table
#'
#' When querying a table from a database,
#' it is helpful to decode the primary and foreign keys
#' in the data frame,
#' i.e. to translate from keys to values.
#' This function provides that service.
#'
#' `schema` is a data model (`dm` object) for the CL-PFU database.
#' It can be obtained from calling `schema_from_conn()`.
#' The default is `schema_from_conn(conn = conn)`,
#' which downloads the `dm` object from `conn`.
#' To save time, pre-compute the `dm` object and
#' supply in the `schema` argument.
#'
#' `fk_parent_tables` is a named list of tables,
#' some of which are foreign key (fk) parent tables for `db_table_name`
#' containing the mapping between fk values (usually strings)
#' and fk keys (usually integers).
#' `fk_parent_tables` is treated as a store from which foreign key tables
#' are retrieved by name when needed.
#' An appropriate value for `fk_parent_tables` can be obtained
#' from `get_all_fk_tables()`.
#'
#' @param .df A data frame whose foreign keys are to be decoded.
#'            Default is `NULL`, meaning that
#'            `.df` should be downloaded from `db_table_name` at `conn`
#'            before decoding foreign keys.
#' @param db_table_name The string name of the database table where `.df` is to be uploaded.
#' @param collect A boolean that tells whether to download the decoded table
#'                (returning an in-memory data frame produced by calling
#'                `dplyr::collect()`) or
#'                a `tbl` (a reference to a database query to be executed
#'                by `dplyr::collect()`).
#'                Default is `FALSE`.
#'                Applies only when `.df` is `NULL`.
#'                Note that to prevent downloads
#'                from `conn`, supply a value for `schema`,
#'                whose default value will download a `dm` object
#'                from the database at `conn`.
#' @param conn An optional database connection.
#'             Necessary only for the default values of `schema` and `fk_parent_tables`.
#' @param schema The data model (`dm` object) for the database in `conn`.
#'               See details.
#' @param fk_parent_tables A named list of all parent tables
#'                         for the foreign keys in `db_table_name`.
#'                         See details.
#' @param .child_table,.child_fk_cols,.parent_key_cols See `PFUPipelineTools::dm_fk_colnames`.
#' @param .pk_suffix See `PFUPipelineTools::key_col_info`.
#' @param .y_joining_suffix The y column name suffix for `left_join()`.
#'                          Default is ".y".
#'
#' @return A version of `.df` with integer keys replaced by key values.
#'
#' @seealso [encode_fks()] for the reverse operation.
#'
#' @export
decode_fks <- function(.df = NULL,
                       db_table_name,
                       conn,
                       collect = FALSE,
                       schema = schema_from_conn(conn),
                       fk_parent_tables = get_all_fk_tables(conn = conn,
                                                            schema = schema),
                       .child_table = PFUPipelineTools::dm_fk_colnames$child_table,
                       .child_fk_cols = PFUPipelineTools::dm_fk_colnames$child_fk_cols,
                       .parent_key_cols = PFUPipelineTools::dm_fk_colnames$parent_key_cols,
                       .pk_suffix = PFUPipelineTools::key_col_info$pk_suffix,
                       .y_joining_suffix = ".y") {

  # if (is.null(.df)) {
  #   .df <- DBI::dbReadTable(conn, db_table_name)
  # }


  if (is.null(.df)) {
    if (collect) {
      .df <- DBI::dbReadTable(conn, db_table_name)
    } else {
      .df <- dplyr::tbl(src = conn, db_table_name)
    }
  }

  # Get details of all foreign keys in .df
  fk_details_for_db_table <- schema |>
    dm::dm_get_all_fks() |>
    dplyr::filter(.data[[.child_table]] == db_table_name) |>
    dplyr::mutate(
      # Remove the <keys> class on child_fk_cols and parent_key_cols.
      "{.child_fk_cols}" := unlist(.data[[.child_fk_cols]]),
      "{.parent_key_cols}" := unlist(.data[[.parent_key_cols]])
    )

  if (nrow(fk_details_for_db_table) == 0) {
    # There are no foreign key columns in .df,
    # so just return .df unmodified.
    return(.df)
  }

  # Get a vector of string names of foreign key columns in .df
  fk_cols_in_df <- fk_details_for_db_table |>
    dplyr::select(dplyr::all_of(.child_fk_cols)) |>
    unlist() |>
    unname()

  for (this_fk_col_in_df in fk_cols_in_df) {
    parent_table_for_this_fk_col_in_df <- fk_details_for_db_table |>
      dplyr::filter(.data[[.child_fk_cols]] == this_fk_col_in_df) |>
      dplyr::select(dplyr::all_of("parent_table")) |>
      unique() |>
      unlist() |>
      unname()
    assertthat::assert_that(length(parent_table_for_this_fk_col_in_df) == 1)

    parent_table_key_col_for_this_fk_col_in_df <- fk_details_for_db_table |>
      dplyr::filter(.data[[.child_fk_cols]] == this_fk_col_in_df) |>
      dplyr::select(dplyr::all_of(.parent_key_cols)) |>
      unique() |>
      unlist() |>
      unname()
    assertthat::assert_that(length(parent_table_key_col_for_this_fk_col_in_df) == 1)

    # strip off the "ID" at the end
    parent_table_value_col_for_this_fk_col_in_df <- sub(
      pattern = paste0(PFUPipelineTools::key_col_info$pk_suffix, "$"),
      replacement = "",
      x = parent_table_key_col_for_this_fk_col_in_df)

    # Get the name of the joined column.
    # There are two possible situations.
    joined_colname <- ifelse(this_fk_col_in_df == parent_table_value_col_for_this_fk_col_in_df,
                             # When the column in .df has the same name as the column in the parent table,
                             # ".y" will be tacked onto the column name
                             yes = paste0(this_fk_col_in_df, .y_joining_suffix),
                             # When the column in .df has a different name as the column in the parent table,
                             # we get simply the parent column name.
                             no = parent_table_value_col_for_this_fk_col_in_df)

    .df <- .df |>
      # Now do the join, which is the process by which we decode the integer
      # in this_fk_col_in_df.
      dplyr::left_join(fk_parent_tables[[parent_table_for_this_fk_col_in_df]],
                       by = dplyr::join_by({{this_fk_col_in_df}} == {{parent_table_key_col_for_this_fk_col_in_df}}),
                       copy = TRUE) |>
      dplyr::mutate(
        "{this_fk_col_in_df}" := .data[[joined_colname]],
        "{joined_colname}" := NULL
      )
  }
  return(.df)
}


