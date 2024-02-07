#' Read a CL-PFU database schema file
#'
#' The schema file is an Excel file that represents the schema for the CL-PFU database.
#' The file is designed to enable easy changes to the CL-PFU database schema
#' for successive versions of the database.
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
#' load_schema_table(version = "v1.4")
load_schema_table <- function(version,
                              schema_path = PFUSetup::get_abs_paths(version = version)[["schema_path"]],
                              schema_sheet = "Schema") {
  schema_path |>
    readxl::read_excel(sheet = schema_sheet)
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
#' load_schema_table(version = "v1.4") |>
#'   schema_dm()
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
  return(dm_tables)
}


load_basic_tables <- function(version,
                              schema_path = PFUSetup::get_abs_paths(version = version)[["schema_path"]]) {

}
