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


#' Load simple tables for the CL-PFU database from a spreadsheet
#'
#' The SchemaAndSimpleTables.xlsx file contains a sheet that
#' represents the schema for the CL-PFU database.
#' The sheet is designed to enable easy changes to the CL-PFU database schema
#' for successive versions of the database.
#' This function reads all simple tables from the SchemaAndSimpleTables.xlsx file
#' and returns a named list of those tables in data frame format.
#'
#' `readme_sheet` and `schema_sheet` are ignored.
#' All other sheets in the file at `schema_path` are assumed to be
#' simple tables that are meant to be loaded into the database.
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
#'
#' @return A named list of data frames, each containing a simple table
#'         of information for the CL-PFU database.
#'
#' @export
load_simple_tables <- function(version,
                               simple_tables_path = PFUSetup::get_abs_paths(version = version)[["schema_path"]],
                               readme_sheet = "README",
                               schema_sheet = "Schema") {
  simple_tables_path |>
    readxl::excel_sheets() |>
    setdiff(c(readme_sheet, schema_sheet)) |>
    self_name() |>
    lapply(FUN = function(this_sheet_name) {
      readxl::read_excel(simple_tables_path, sheet = this_sheet_name)
    })
}


#' Create a data model from an Excel schema table
#'
#' This function returns a `dm` object suitable for
#' future uploading to a DBMS.
#'
#' `schema_table` is assumed to be a data frame with the following columns:
#'
#'   * `.table`: gives table names in the database.
#'   * `.colname`: gives column names in each table; if suffixed with `pk_suffix`,
#'                 interpreted as a primary key column.
#'   * `.is_pk`: tells if `.colname` is a primary key for `.table`.
#'   * `.coldatatype`: gives the data type for the column,
#'                     one of "int", "boolean", "text", or "double precision".
#'   * fk.table: gives a table in which the foreign key can be found
#'   * fk.colname: gives the column name in fk.table where the foreign key can be found
#'
#' @param schema_table A schema table, typically the output of `load_schema_table()`.
#' @param pk_suffix The suffix for primary keys.
#'                  Default is "_ID".
#' @param .table,.colname,.is_pk,.coldatatype See `PFUPipelineTools::schema_table_colnames`.
#' @param .pk_cols Column names used internally.
#'
#' @return A `dm` object created from `schema_table`.
#'
#' @export
#'
#' @examples
#' st <- tibble::tribble(~Table, ~colname, ~coldatatype, ~fk.table, ~fk.colname,
#'                       "Country", "Country_ID", "int", "NA", "NA",
#'                       "Country", "Country", "text", "NA", "NA",
#'                       "Country", "Description", "text", "NA", "NA")
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
          stop(paste0("Unknown data type: '", this_data_type, "' in schema_dm"))
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
#' @param drop_db_tables A boolean that tells whether to delete
#'                       existing tables before uploading the new schema.
#'
#' @return The remote data model
#'
#' @export
pl_upload_schema_and_simple_tables <- function(schema,
                                               simple_tables,
                                               conn,
                                               set_not_null_constraints = TRUE,
                                               drop_db_tables = FALSE) {
  # Get rid of the tables, if desired
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
      magrittr::extract2("pk_col") |>
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
#'
#' @return `NULL` silently.
#'
#' @export
set_not_null_constraints_on_fk_cols <- function(schema, conn) {
  # Set NOT NULL constraint on all foreign key columns
  schema |>
    dm::dm_get_all_fks() |>
    dplyr::mutate(
      # Convert <keys> to <chr>
      child_fk_cols = unlist(child_fk_cols)
    ) |>
    purrr::pmap(.f = function(child_table, child_fk_cols, parent_table, parent_key_cols, on_delete) {
      stmt <- paste0('ALTER TABLE "', child_table,
                    '" ALTER COLUMN "', child_fk_cols,
                    '" set NOT NULL;')
      DBI::dbExecute(conn, stmt)
    })
  return(invisible(NULL))
}

#' Upsert a data frame with optional recoding of foreign keys
#'
#' Upserts
#' (inserts or updates,
#' depending on whether the information already exists in `db_table_name`)
#' `.df` into `db_table_name` at `conn`.
#' This function decodes foreign keys, when possible,
#' by assuming that all keys are integers.
#' If non-integers are provided in foreign key columns of `.df`,
#' the non-integers will be recoded to the integer key values.
#' Thus, this function assumes that the data model and schema
#' already exists in `conn`.
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
#'     * CLPFUDBTable: A column of character strings,
#'                     all with the value of `db_table_name`.
#'     * All foreign key columns: With their integer values (to save space).
#'     * Hash: A column with a hash of all non-foreign-key columns.
#'
#' `schema` is a data model (`dm` object) for the database in `conn`.
#' Its default value (`dm::dm_from_con(conn, learn_keys = TRUE)`)
#' extracts the data model for the database at `conn` automatically.
#' However, if the caller already has the data model,
#' supplying it in the `schema` argument will save time.
#'
#' `parent_tables` is a named list of tables,
#' one of which (the one named `db_table_name`)
#' contains the foreign keys for `db_table_name`.
#' `parent_tables` is treated as a store from which foreign key tables
#' are retrieved by name when needed.
#' The default value (which is several lines of code)
#' retrieves all possible foreign key parent tables from conn,
#' potentially a time-consuming process.
#' For speed, pre-compute all foreign key parent tables once
#' and pass the list to the `parent_tables` argument
#' of all similar functions.
#'
#' The user in `conn` must have write access to the database.
#'
#' @param .df The data frame to be upserted.
#' @param db_table_name A string identifying the destination for `.df`,
#'                      the name of a remote database table in `conn`.
#' @param conn A connection to the CL-PFU database.
#' @param in_place A boolean that tells whether to modify the database at `conn`.
#'                 Default is `FALSE`, which is helpful if you want to chain
#'                 several requests.
#' @param schema The data model (`dm` object) for the database in `conn`.
#'               Default is `dm::dm_from_con(conn, learn_keys = TRUE)`.
#'               See details.
#' @param fk_parent_tables A named list of all parent tables
#'                         for the foreign keys in `db_table_name`.
#'                         See details.
#'
#' @return A special hash of `.df`. See details.
#'
#' @seealso `pl_download()` for the reverse operation.
#'          `pl_upload_schema_and_simple_tables()` for a way to establish the database schema.
#'
#' @export
pl_upsert <- function(.df,
                      db_table_name,
                      conn,
                      in_place = FALSE,
                      schema = dm::dm_from_con(conn, learn_keys = TRUE),
                      fk_parent_tables = PFUPipelineTools::get_all_fk_tables(conn = conn, schema = schema)) {

  pk_table <- dm::dm_get_all_pks(schema, table = {{db_table_name}})
  # Make sure we have one and only one primary key column
  assertthat::assert_that(nrow(pk_table) == 1,
                          msg = paste0("Table '",
                                       db_table_name,
                                       "' has ", nrow(pk_table),
                                       " primary keys. 1 is required."))
  # Get the primary key name as a string
  pk_str <- pk_table |>
    # "pk_col" is the name of the column of primary key names
    # in the tibble returned by dm::dm_get_all_pks()
    magrittr::extract2("pk_col") |>
    magrittr::extract2(1)

  # Replace fk column values in .df with integer keys, if needed.
  recoded_df <- .df |>
    code_fks(db_table_name = db_table_name,
             schema = schema,
             fk_parent_tables = fk_parent_tables)

  dplyr::tbl(conn, db_table_name) |>
    dplyr::rows_upsert(recoded_df,
                       by = pk_str,
                       copy = TRUE,
                       in_place = in_place)
}


#' Code foreign keys in a data frame to be uploaded
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
#' It can be obtained from code such as
#' `dm::dm_from_con(con = << a database connection >>, learn_keys = TRUE)`.
#'
#' `fk_parent_tables` is a named list of tables,
#' some of which are fk parent tables containing
#' the mapping between fk values (usually string)
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
#' @param schema The data model (`dm` object) for the database in `conn`.
#'               See details.
#' @param fk_parent_tables A named list of all parent tables
#'                         for the foreign keys in `db_table_name`.
#'                         See details.
#'
#' @seealso [decode_keys()] for the inverse operation,
#'          albeit for all keys, primary and foreign.
#'
#' @return A version of `.df` with fk values (often strings)
#'         replaced by fk keys (integers).
#'
#' @export
code_fks <- function(.df,
                     db_table_name,
                     schema,
                     fk_parent_tables) {

  # Get details of all foreign keys in .df
  fk_details_for_db_table <- schema |>
    dm::dm_get_all_fks() |>
    dplyr::filter(.data[["child_table"]] == db_table_name) |>
    dplyr::mutate(
      # Remove the <keys> class on child_fk_cols and parent_key_cols.
      child_fk_cols = unlist(child_fk_cols),
      parent_key_cols = unlist(parent_key_cols)
    )

  if (nrow(fk_details_for_db_table) == 0) {
    # There are no foreign key columns in .df,
    # so just return .df unmodified.
    return(.df)
  }

  # Get a vector of string names of foreign key columns in .df
  fk_cols_in_df <- fk_details_for_db_table |>
    dplyr::select(child_fk_cols) |>
    unlist() |>
    unname()

  for (this_fk_col_in_df in fk_cols_in_df) {
    if (is.integer(.df[[this_fk_col_in_df]])) {
      # This is already an integer, and
      # presumably no need to recode
      next
    }
    # Get the parent levels
    fk_levels <- fk_parent_tables[[this_fk_col_in_df]] |>
      dplyr::arrange(.data[[paste0(this_fk_col_in_df, "ID")]]) |>
      dplyr::select(dplyr::all_of(this_fk_col_in_df)) |>
      unlist() |>
      unname()
    .df <- .df |>
      dplyr::mutate(
        "{this_fk_col_in_df}" := factor(.data[[this_fk_col_in_df]], levels = fk_levels),
        "{this_fk_col_in_df}" := as.integer(.data[[this_fk_col_in_df]])
      )
  }

  return(.df)
}


#' Decode keys in a downloaded data frame
#'
#' When downloading a data frame from a database,
#' it is helpful to decode the primary and foreign keys
#' in the data frame,
#' i.e. to translate from keys to values.
#' This function provides that service.
#'
#' `schema` is a data model (`dm` object) for the CL-PFU database.
#' It can be obtained from code such as
#' `dm::dm_from_con(con = << a database connection >>, learn_keys = TRUE)`.
#'
#' @param .df The data frame about to be uploaded.
#' @param db_table_name The string name of the database table where `.df` is to be uploaded.
#' @param schema The data model (`dm` object) for the database in `conn`.
#'               See details.
#'
#' @return
#' @export
#'
#' @examples
decode_keys <- function(.df,
                        db_table_name,
                        schema,
                        fk_parent_tables) {

  # Get details of all foreign keys in .df
  fk_details_for_db_table <- schema |>
    dm::dm_get_all_fks() |>
    dplyr::filter(.data[["child_table"]] == db_table_name) |>
    dplyr::mutate(
      # Remove the <keys> class on child_fk_cols and parent_key_cols.
      child_fk_cols = unlist(child_fk_cols),
      parent_key_cols = unlist(parent_key_cols)
    )

  if (nrow(fk_details_for_db_table) == 0) {
    # There are no foreign key columns in .df,
    # so just return .df unmodified.
    return(.df)
  }

  # Get a vector of string names of foreign key columns in .df
  fk_cols_in_df <- fk_details_for_db_table |>
    dplyr::select(child_fk_cols) |>
    unlist() |>
    unname()

  for (this_fk_col_in_df in fk_cols_in_df) {
    joined_colname <- paste0(this_fk_col_in_df, ".y")
    tablenameID <- paste0(this_fk_col_in_df, "ID")
    .df <- .df |>
      dplyr::left_join(fk_parent_tables[[this_fk_col_in_df]],
                       by = dplyr::join_by({{this_fk_col_in_df}} == {{tablenameID}})) |>
      dplyr::mutate(
        "{this_fk_col_in_df}" := .data[[joined_colname]],
        "{joined_colname}" := NULL
      )
  }
  return(.df)
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
                                       drop_db_tables = TRUE)
}
