#' Save a target to a pinboard.
#'
#' Releases (`release = TRUE`)
#' or not (`release = FALSE`)
#' a new version of `targ`
#' using the `pins` package.
#'
#' Released versions of the target can be obtained
#' as shown in examples.
#'
#' @param pipeline_releases_folder The folder that contains the pinboard for releases from the pipeline.
#' @param targ The target R object to be saved to the pinboard.
#' @param pin_name The name of the pin in the pinboard. `pin_name` is the key to retrieving `targ`.
#' @param type The type of the target, routed to `pins::pin_write()`. Default is "rds". "csv" in another option.
#' @param release A boolean telling whether to do a release.
#'                Default is `FALSE`.
#'
#' @return If `release` is `TRUE`,
#'         the fully-qualified path name of the `targ` file in the pinboard.
#'         If `release` is `FALSE`, the string "Release not requested."
#'
#' @export
#'
#' @examples
#' \dontrun{
#' # Establish the pinboard
#' pinboard <- file.path("~",
#'                       "Dropbox",
#'                       "Fellowship 1960-2015 PFU database",
#'                       "OutputData", "PipelineReleases") |>
#'   pins::board_folder()
#' # Get information about the `PSUT` target in the pinboard
#' pinboard |>
#'   pins::pin_meta(name = "psut")
#' # Find versions of the `PSUT` target
#' pinboard |>
#'   pins::pin_versions(name = "psut")
#' # Get the latest copy of the `PSUT` target.
#' my_psut <- pinboard |>
#'   pins::pin_read(name = "psut")
#' # Retrieve a previous version of the `PSUT` target.
#' my_old_psut <- pinboard |>
#'   pins::pin_read(name = "psut", version = "20220218T023112Z-1d9e1")
#' }
release_target <- function(pipeline_releases_folder, targ, pin_name, type = "rds", release = FALSE) {
  if (release) {
    # Establish the pinboard
    out <- pins::board_folder(pipeline_releases_folder, versioned = TRUE) |>
      # Returns the fully-qualified name of the file written to the pinboard.
      pins::pin_write(targ, name = pin_name, type = type, versioned = TRUE)
  } else {
    out <- "Release not requested."
  }
  return(out)
}


#' Calculate hash of pipeline data
#'
#' In the CL-PFU database pipeline,
#' we need the ability to download a
#' data frame from the database
#' based on a hash of the data.
#' This function calculates the appropriate hash.
#'
#' The hash has two requirements:
#'
#'   - values change when content changes and
#'   - provides sufficient information to retrieve the real data frame.
#'
#' The uploaded data frames may be created by
#' grouped calculations,
#' so some columns are likely to have only one unique value.
#' We need to retain data of those single-valued columns
#' as well as the name of the database table.
#'
#' To meet those requirements,
#' the return value from this function
#' has the following characteristics:
#'
#'   - The first column (named with the value of `.table_name_col`)
#'     contains the value of `table_name`, the name
#'     of the database table where `.df` is stored.
#'   - The second through N-1 columns are
#'     all columns with only one unique value AND those columns specified by
#'     `additional_hash_group_cols`.
#'     If `additional_hash_group_cols` is `NULL` (the default),
#'     grouping is done on only those columns with one unique value.
#'   - The Nth column (named with the value of `.nested_col`)
#'     contains a hash of a data frame created by nesting
#'     by all columns with more than one unique value and
#'     `additional_hash_group_cols` (when `additional_hash_group_cols`
#'     is not `NULL`).
#'
#' The return value serves as a "ticket" with which
#' data can be retrieved from the database at a later time.
#' When data are withdrawn from the database,
#' the first column can be removed, and
#' the other columns with one unique value
#' can be used to filter `table_name`.
#'
#' Internally, this function switches on the result of [dplyr::is.tbl()].
#' When `TRUE`, a hashed version of the table is downloaded.
#' When `FALSE`, a hashed version of the table is calculated.
#' Both approaches use the `md5` hashing algorithm.
#'
#' If the `.table_name_column` is not present in `.df`,
#' it is added internally, filled with the value of `table_column`.
#'
#' @param .df A data frame to be stored in the database or
#'            a `tbl` for a table in a remote database.
#' @param table_name The name of the table in which `.df` will be stored.
#'                   This argument can be `NULL` (the default)
#'                   if `.table_name_col` is present in `.df`.
#' @param additional_hash_group_cols The additional columns by which `.df` will be grouped
#'                                   before making the `.nested_hash_col` hash column.
#'                                   Default is `NULL`, meaning that grouping will be
#'                                   done on all columns that contain only 1 unique value.
#'                                   See details.
#' @param .table_name_col The name of the column of the output that contains `table_name` on output.
#'                        Default is `PFUPipelineTools::hashed_table_colnames$db_table_name`.
#' @param conn A connection to a database.
#'             Necessary only if `.df` is a `.tbl`.
#' @param .nested_col The name of the column of the output that contains
#'                     nested data.
#'                     Default is `PFUPipelineTools::hashed_table_colnames$nested_col_name`.
#' @param .nested_hash_col The name of the column of the output that contains
#'                         the hash of nested columns.
#'                         Default is `PFUPipelineTools::hashed_table_colnames$nested_hash_col_name`.
#' @param .algo The algorithm for hashing.
#'              Default is "md5".
#'
#' @return A modified version of `.df` without groups. See details.
#'
#' @export
pl_hash <- function(.df,
                    table_name = NULL,
                    additional_hash_group_cols = NULL,
                    conn,
                    .table_name_col = PFUPipelineTools::hashed_table_colnames$db_table_name,
                    .nested_col = PFUPipelineTools::hashed_table_colnames$nested_col_name,
                    .nested_hash_col = PFUPipelineTools::hashed_table_colnames$nested_hash_col_name,
                    .algo = "md5") {
  if (!is.null(table_name)) {
    # Make sure the table_name has length 1.
    if (length(table_name) != 1) {
      stop("length(table_name) must be 1 in pl_hash()")
    }
  }

  if (!dplyr::is.tbl(.df)) {
    # We have an in-memory data frame
    if (!(table_name %in% colnames(.df))) {
      # Set the table name column
      .df <- .df |>
        dplyr::mutate(
          "{.table_name_col}" := table_name,
        ) |>
        # Move the table name column to the left of the data frame
        dplyr::relocate(dplyr::all_of(.table_name_col))
    }
    # Figure out names of columns that have only 1 unique value.
    # We will group by these columns before nesting and hashing.
    single_value_cols <- .df |>
      sapply(function(this_col) {
        length(unique(this_col)) == 1
      })
    hash_group_cols <- names(single_value_cols[single_value_cols])

    if (!is.null(additional_hash_group_cols)) {
      # Eliminate any strings that don't have a corresponding column in .df
      additional_hash_group_cols_not_present <- setdiff(colnames(.df), additional_hash_group_cols)
      additional_hash_group_cols <- setdiff(colnames(.df), additional_hash_group_cols_not_present)
      # Now add to hash_group_cols
      hash_group_cols <- hash_group_cols |>
        append(additional_hash_group_cols) |>
        unique()
    }
    out <- .df |>
      # Group by single_value_cols
      dplyr::group_by(dplyr::across(dplyr::all_of(hash_group_cols))) |>
      # Nest
      tidyr::nest(.key = .nested_col) |>
      # Calculate hash
      dplyr::mutate(
        "{.nested_hash_col}" := digest::digest(.data[[.nested_col]], algo = .algo),
        "{.nested_col}" := NULL
      ) |>
      dplyr::ungroup()
  } else {
    # We have a tbl reference to a data frame in a database.
    # Build the SQL statement that will pull down the hashed table

    # First find the columns with only 1 unique value.
    # These are columns to nest, unless
    # they are in additional_hash_group_cols.
    unique_cols <- unique_cols_in_tbl(table_name = table_name, conn = conn)

    # All column names NOT in the additional hash group columns
    # are columns to nest.
    cols_to_nest <- setdiff(unique_cols, additional_hash_group_cols)

    # Build the query to return a hashed table
    #
    # Here is an example query

    # SELECT
    # "Country",
    # jsonb_build_object(
    #   'Year', jsonb_agg("Year"),
    #   'X', jsonb_agg("X")
    # ) AS "NestedColumn"
    # FROM
    # "source"
    # GROUP BY
    # "Country";

    # SELECT
    # "Country",
    # array_agg("Year") AS "Year",
    # array_agg("Value") AS "Value"
    # FROM
    # "TestPLHash"
    # GROUP BY
    # "Country";

    # SELECT
    # "Country",
    # array_agg("Year")::text AS "Year",
    # array_agg("Value")::text AS "Value"
    # FROM
    # "TestPLHash"
    # GROUP BY
    # "Country";

    countries_string <- paste0("(", paste0(countries_encoded, collapse = ", "), ")")

    cols_clause <- additional_hash_group_cols

    select_clause <- 'SELECT '

  }


  return(out)
}


unique_cols_in_tbl <- function(table_name, conn) {
  # Get the tbl
  this_tbl <- dplyr::tbl(conn, table_name)
  # The SQL query looks like this:

  # SELECT
  # COUNT(DISTINCT "Country") AS UniqueCountries,
  # COUNT(DISTINCT "Year") AS UniqueYears,
  # COUNT(DISTINCT "EnergyType") AS UniqueEnergyTypes,
  # COUNT(DISTINCT "Value") AS UniqueValues
  # FROM
  # "TestPLHash";

  # Get the count clause based on the the column names in the table
  count_clause <- colnames(this_tbl) |>
    sapply(FUN = function(cname){
      paste0('COUNT(DISTINCT "', cname, '") AS "', cname, '" ')
    }) |>
    paste0(collapse = ', ')
  # Build the SQL query
  query <- paste0('SELECT ', count_clause, ' FROM "', table_name, '"')
  # Get a 1-row data frame with existing columns and the number of unique values
  count_df <- DBI::dbGetQuery(conn, query)
  # Finally, get the names of the columns where the value is 1
  names(count_df)[count_df == 1]
}





















