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
#' load_schema_table(version = "v1.4")
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
  simple_table_sheet_names <- simple_tables_path |>
    readxl::excel_sheets() |>
    setdiff(c(readme_sheet, schema_sheet))
  simple_table_sheet_names |>
    stats::setNames(simple_table_sheet_names) |>
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
#'   * Table: gives table names in the database
#'   * colname: gives column names in each table; if suffixed with `pk_suffix`,
#'              interpreted as a primary key column
#'   * coldatatype: gives the data type for the column,
#'                  one of "int", "boolean", "text", or "double precision"
#'   * fk.table: gives a table in which the foreign key can be found
#'   * fk.colname: gives the column name in fk.table where the foreign key can be found
#'
#' @param schema_table A schema table, typically the output of `load_schema_table()`.
#' @param pk_suffix The suffix for primary keys.
#'                  Default is "_ID"
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
schema_dm <- function(schema_table, pk_suffix = PFUPipelineTools::key_col_info$pk_suffix) {

  dm_table_names <- schema_table[["Table"]] |>
    unique()
  dm_tables <- dm_table_names |>
    stats::setNames(dm_table_names) |>
    lapply(FUN = function(this_table_name) {
      colnames <- schema_table |>
        dplyr::filter(.data[["Table"]] == this_table_name) |>
        magrittr::extract2("colname")
      # Convert to a data frame
      this_mat <- matrix(nrow = 0, ncol = length(colnames), dimnames = list(NULL, colnames))
      this_dm_table <- this_mat |>
        as.data.frame() |>
        tibble::as_tibble()
      # Set data types
      coldatatypes <- schema_table |>
        dplyr::filter(.data[["Table"]] == this_table_name) |>
        magrittr::extract2("coldatatype")
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

  # Set primary keys according to the convention
  # that the primary key column ends with pk_suffix
  primary_key_colnames <- dm_tables |>
    lapply(FUN = function(this_table) {
      cnames <- colnames(this_table)
      cnames[[which(endsWith(cnames, pk_suffix), arr.ind = TRUE)]]
    })

  for (itbl in 1:length(dm_table_names)) {
    this_table_name <- dm_table_names[[itbl]]
    this_primary_key_colname <- primary_key_colnames[[itbl]]
    dm_tables <- dm_tables |>
      dm::dm_add_pk(table = {{this_table_name}},
                    columns = {{this_primary_key_colname}},
                    autoincrement = TRUE)
  }

  # Set foreign keys according to the schema_table
  fk_info <- schema_table |>
    dplyr::filter(.data[["fk.colname"]] != "NA")

  if (nrow(fk_info) > 0) {
    for (irow in 1:nrow(fk_info)) {
      this_table_name <- fk_info[["Table"]][[irow]]
      colname <- fk_info[["colname"]][[irow]]
      fk_table <- fk_info[["fk.table"]][[irow]]
      fk_colname <- fk_info[["fk.colname"]][[irow]]
      dm_tables <- dm_tables |>
        dm::dm_add_fk(table = {{this_table_name}},
                      columns = {{colname}},
                      ref_table = {{fk_table}},
                      ref_columns = {{fk_colname}})
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
#' `simple_tables` should not include any foreign keys,
#' because the order for uploading `simple_tables` is not guaranteed
#' to avoid uploading a table with a foreign key before
#' the table containing the foreign key is available.
#'
#' `conn`'s user must have superuser privileges.
#'
#' @param schema A data model (a `dm` object).
#' @param simple_tables A named list of data frames with the content of
#'                      tables in `conn`.
#' @param conn A `DBI` connection to a database.
#' @param drop_db_tables A boolean that tells whether to delete
#'                       existing tables before uploading the new schema.
#'
#' @return The remote data model
#'
#' @export
pl_upload_schema_and_simple_tables <- function(schema,
                                            simple_tables,
                                            conn,
                                            drop_db_tables = FALSE) {
  # Get rid of the tables, if desired
  pl_destroy(conn, destroy_cache = FALSE, drop_tables = drop_db_tables)
  # Copy the data model to conn
  dm::copy_dm_to(conn, schema, temporary = FALSE)
  # Upload the simple tables
  names(simple_tables) |>
    purrr::map(function(this_table_name) {
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
    })
}


#' Upsert a data frame with optional recoding of foreign keys
#'
#' Upserts
#' (inserts or updates,
#' depending on whether the information already exists in `remote_table`)
#' `.df` into `remote_table` at `conn`.
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
#'     * CLPFUDBTable: A column of character strings, all with the value of `remote_table`.
#'     * All foreign key columns: With their integer values (to save space).
#'     * Hash: A column with a hash of all non-foreign-key columns.
#'
#' `schema` is a data model (`dm` object) for the database in `conn`.
#' Its default value (`dm::dm_from_con(conn, learn_keys = TRUE)`)
#' extracts the data model for the database at `conn` automatically.
#' However, if the caller already has the data model,
#' supplying it in the `schema` argument will save time.
#'
#' The user in `conn` must have write access to the database.
#'
#' @param .df The data frame to be upserted.
#' @param remote_table_name A string identifying the destination for `.df`,
#'                          the name of a remote database table in `conn`.
#' @param conn A connection to the CL-PFU database.
#' @param schema The data model (`dm` object) for the database in `conn`.
#'               Default is `dm::dm_from_con(conn, learn_keys = TRUE)`.
#'               See details.
#'
#' @return A special hash of `.df`. See details.
#'
#' @seealso `pl_download()` for the reverse operation.
#'          `pl_upload_schema_and_simple_tables()` for a way to establish the database schema.
#'
#' @export
pl_upsert <- function(.df,
                      remote_table_name,
                      conn,
                      schema = dm::dm_from_con(conn, learn_keys = TRUE)) {

  pk_table <- dm::dm_get_all_pks(schema, table = {{remote_table_name}})
  # Make sure we have one and only one primary key column
  assertthat::assert_that(nrow(pk_table) == 1,
                          msg = paste0("Table '",
                                       remote_table_name,
                                       "' has ", nrow(pk_table),
                                       " primary keys. 1 is required."))
  # Get the primary key name as a string
  pk_str <- pk_table |>
    # "pk_col" is the name of the column of primary key names
    # in the tibble returned by dm::dm_get_all_pks()
    magrittr::extract2("pk_col") |>
    magrittr::extract2(1)

  # Replace fk column values in .df with keys, if needed

  recoded_df <- .df |>
    recode_fks(remote_table_name = remote_table_name, conn = conn, schema = schema)

  dplyr::tbl(conn, remote_table_name) |>
    dplyr::rows_upsert(recoded_df,
                       by = pk_str,
                       copy = TRUE,
                       in_place = TRUE)
}


#' Recode foreign keys in a data frame to be uploaded
#'
#' In the CL-PFU pipeline,
#' we allow data frames about to be uploaded
#' to have foreign key values (not integers)
#' in foreign key columns.
#' This function translates the foreign key values
#' to integers.
#'
#' `schema` is a data model (`dm` object) for the database in `conn`.
#' Its default value (`dm::dm_from_con(conn, learn_keys = TRUE)`)
#' extracts the data model for the database at `conn` automatically.
#' However, if the caller already has the data model,
#' supplying it in the `schema` argument will save time.
#'
#' If any values in a foreign key column of `.df`
#' does not have a corresponding integer,
#' an error is thrown.
#'
#' @param .df The data frame about to be uploaded.
#' @param remote_table_name The string name of the remote table in `conn` where `.df` is to be uploaded.
#' @param conn The connection in which `remote_table_name` resides.
#' @param schema The data model (`dm` object) for the database in `conn`.
#'               Default is `dm::dm_from_con(conn, learn_keys = TRUE)`.
#'               See details.
#'
#' @return A version of `.df` with foreign key columns guaranteed to be integers.
#'
#' @export
recode_fks <- function(.df,
                       remote_table_name,
                       conn,
                       schema = dm::dm_from_con(conn, learn_keys = TRUE)) {

  # Get details of all foreign keys in .df
  fk_details_for_remote_table_name <- dm::dm_get_all_fks(schema) |>
    dplyr::filter(.data[["child_table"]] == remote_table_name)
  if (nrow(fk_details_for_remote_table_name) == 0) {
    # There are no foreign key columns in .df,
    # so just return .df unmodified.
    return(.df)
  }

  # Get a vector of string names of foreign key columns in .df
  fk_cols_in_df <- fk_details_for_remote_table_name |>
    magrittr::extract2("child_fk_cols") |>
    unlist()

  # Map over these names, recoding fk columns
  # to integers, if necessary.
  fk_cols_in_df |>
    purrr::map(function(this_fk_col) {
      if (is.integer(.df[[this_fk_col]])) {
        # this_fk_col is already an integer,
        # do nothing.
      } else {
        # fk_parent_table_name <- fk_details_for_remote_table_name |>
        #   dplyr::filter(.data[["child_table"]] == remote_table_name) |>
        #   magrittr::extract2("parent_table")
        #
        # fk_table <- dplyr::tbl(conn, fk_parent_table_name) |>
        #   dplyr::collect()
        # # Set join specification details.
        # # x column to join by is this_fk_col
        # # y column to join by is this_fk_col less the fk suffix
        #
        #
        #
        # #
        # # Maybe instead do a levels trick.
        # #
        #
        #
        #
        #
        # join_by_x <- this_fk_col
        # join_by_y <- sub(pattern = paste0("_ID", "$"),
        #                  replacement = "",
        #                  x = join_by_x)
        # joined.y <- paste0(join_by_x, ".y")
        # # Left join with .df using the join_by details
        # .df <- dplyr::left_join(.df,
        #                         fk_table,
        #                         by = dplyr::join_by({{join_by_x}} == {{join_by_y}})) |>
        #   # Replace the non-integer column with the new integer column
        #   dplyr::mutate(
        #     "{join_by_x}" := .data[[joined.y]],
        #     "{joined.y}" := NULL
        #   )
        fk_parent_table_name <- fk_details_for_remote_table_name |>
          dplyr::filter(.data[["child_table"]] == remote_table_name) |>
          magrittr::extract2("parent_table")


        # Get a data frame of foreign keys for remote_table_name
        fks_for_remote_table_name <- schema |>
          dm::dm_get_all_fks() |>
          dplyr::filter(.data[["child_table"]] == remote_table_name)

        if (nrow(fks_for_remote_table_name) == 0) {
          # There are no foreign key columns in .df,
          # so just return .df unmodified.
          return(.df)
        }

        # Get a vector of string names of foreign key columns in .df
        fk_cols_in_df <- fks_for_remote_table_name |>
          magrittr::extract2("child_fk_cols") |>
          unlist()

        # Map over these names, recoding fk columns
        # to integers, if necessary.
        fk_cols_in_df |>
          purrr::map(function(this_fk_col) {
            fk_parent_table_name <- fks_for_remote_table_name |>
              # Get the foreign table name
              dplyr::filter(.data[["child_table"]] == remote_table_name) |>
              magrittr::extract2("parent_table")
            # Get the name of the column in fk_parent_table_name
            # that contains the foreign key
            fk_colname_in_foreign_table <- fks_for_remote_table_name |>
              dplyr::filter(.data[["child_table"]] == remote_table_name) |>
              magrittr::extract2("parent_key_cols") |>
              unlist()
            # Download the table that contains the actual foreign key
            fk_levels <- dplyr::tbl(conn, fk_parent_table_name) |>
              dplyr::collect() |>
              # Arrange it by fk_colname_in_foreign_table
              # to generate ordered levels
              dplyr::arrange(fk_colname_in_foreign_table) |>
              # Extract the column that is NOT the foreign key.
              # This is probably a column of names, but they are the
              # levels we will need.
              dplyr::select(-{{this_fk_col}}) |>
              unlist() |>
              unname()
            # Change this_fk_col to a factor with those levels.
            .df <- .df |>
              dplyr::mutate(
                "{this_fk_col}" := factor(.data[[this_fk_col]], levels = fk_levels),
                "{this_fk_col}" := as.integer(.data[[this_fk_col]])
              )
          })
        }
    })

  return(.df)
}


#' Upload a small database of Beatles information
#'
#' Used only for testing.
#'
#' @param conn The connection to a Postgres database. Must have write permission.
#'
#' @return A list of tables containing Beatles information
upload_beatles <- function(conn) {
  # Avoid some notes in R CMD check
  Member_ID <- NULL
  Member <- NULL
  Role <- NULL
  Members <- NULL
  Roles <- NULL

  # Band members table specification (no data)
  mems <- matrix(nrow = 0, ncol = 2, dimnames = list(c(), c("Member_ID", "Member"))) |>
    as.data.frame() |>
    dplyr::mutate(
      Member_ID = as.integer(Member_ID),
      Member = as.character(Member)
    )
  # Band roles table specification (no data)
  rls <- matrix(nrow = 0, ncol = 2, dimnames = list(c(), c("Member_ID", "Role"))) |>
    as.data.frame() |>
    dplyr::mutate(
      Member_ID = as.integer(Member_ID),
      Role = as.character(Role)
    )

  # Build the data model
  DM <- list(Members = mems, Roles = rls) |>
    dm::as_dm() |>
    dm::dm_add_pk(table = Members, columns = Member_ID, autoincrement = TRUE) |>
    dm::dm_add_pk(table = Roles, columns = Member_ID, autoincrement = TRUE) |>
    dm::dm_add_fk(table = Roles, columns = Member_ID, ref_table = Members, ref_columns = Member_ID)

  # Create tables to add to the DM
  members <- data.frame(Member_ID = as.integer(1:4),
                        Member = c("John Lennon", "Paul McCartney", "George Harrison", "Ringo Starr"))
  roles <- data.frame(Member_ID = as.integer(1:4),
                      Role = c("Lead singer", "Bassist", "Guitarist", "Drummer"))
  tables_to_add <- list(Members = members, Roles = roles)
  pl_upload_schema_and_simple_tables(schema = DM,
                                  simple_tables = tables_to_add,
                                  conn = conn,
                                  drop_db_tables = TRUE)
}
