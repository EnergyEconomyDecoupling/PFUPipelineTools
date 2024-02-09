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
schema_dm <- function(schema_table, pk_suffix = "_ID") {

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
#' the schema (`.dm`) and simple tables (`simple_tables`).
#' `drop_db_tables` is `FALSE` by default.
#' However, it is unlikely that this function will succeed unless
#' `drop_db_tables` is set `TRUE`, because
#' uploading the data model `.dm` to `conn` will fail
#' if the tables already exist in the database at `conn`.
#'
#' `simple_tables` should not include any foreign keys,
#' because the order for uploading `simple_tables` is not guaranteed
#' to avoid uploading a table with a foreign key before
#' the table containing the foreign key is available.
#'
#' `conn`'s user must have superuser privileges.
#'
#' @param .dm A data model (a `dm` object).
#' @param simple_tables A named list of data frames with the content of
#'                      tables in `conn`.
#' @param conn A `DBI` connection to a database.
#' @param drop_db_tables A boolean that tells whether to delete
#'                       existing tables before uploading the new schema.
#'
#' @return The remote data model
#'
#' @export
upload_schema_and_simple_tables <- function(.dm, simple_tables, conn, drop_db_tables = FALSE) {
  pl_destroy(conn, destroy_cache = FALSE, drop_tables = drop_db_tables)
  dm::copy_dm_to(conn, .dm, temporary = FALSE)
  # # Get the remote data model
  # remote_dm <- dm::dm_from_con(conn, table_names = names(.dm), learn_keys = TRUE)
  # # Add tables
  # dm::dm_rows_append(remote_dm,
  #                    dm::as_dm(simple_tables),
  #                    in_place = TRUE)

  names(simple_tables) |>
    purrr::map(function(this_table_name) {
      DBI::dbAppendTable(conn, this_table_name, simple_tables[[this_table_name]])
    })

}






