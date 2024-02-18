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


#' Calculate has of pipeline data
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
#' We need to know retain data of those single-valued columns
#' as well as the name of the database table.
#'
#' To meet those requirements,
#' the return value from this function
#' has the following characteristics:
#'
#'   - The first column (named with the value of `.table_name_col`)
#'     contains the value of `table_name`, the name
#'     of the database table where `.df` is stored.
#'   - The second through N-1 columns are others with only one unique value.
#'   - The Nth column (named with the value of `.nested_col`)
#'     contains a hash of a data frame created by nesting
#'     all columns with more than one unique value.
#'
#' When data are withdrawn from the database at a later time,
#' the first column can be removed, and
#' the other columns with one unique
#' value can be filtered.
#'
#' @param .df The data frame to be stored in the database.
#' @param table_name The name of the table in which `.df` will be stored.
#' @param .table_name_col The name of the column of the output that contains `table_name` on output.
#'                        Default is `PFUPipelineTools::hashed_table_colnames$db_table_name`.
#' @param .nested_col The name of the column of the output that contains
#'                    the has of nested columns.
#'                    Default is `PFUPipelineTools::hashed_table_colnames$nested_col_name`.
#' @param .algo The algorithm for hashing.
#'              Default is "md5".
#'
#' @return A modified version of `.df` without groups. See details.
#'
#' @export
pl_hash <- function(.df,
                    table_name,
                    .table_name_col = PFUPipelineTools::hashed_table_colnames$db_table_name,
                    .nested_col = PFUPipelineTools::hashed_table_colnames$nested_col_name,
                    .nested_hash_col = PFUPipelineTools::hashed_table_colnames$nested_hash_col_name,
                    .algo = "md5") {
  # Set the table name
  out <- .df |>
    dplyr::mutate(
      "{.table_name_col}" := table_name,
    ) |>
    # Move the table name column to the left of the data frame
    dplyr::relocate(dplyr::all_of(.table_name_col))

  # Figure out names of columns that have only 1 unique value.
  single_value_cols <- out |>
    sapply(function(this_col) {
      length(unique(this_col)) == 1
    })
  single_value_col_names <- names(single_value_cols[single_value_cols])

  out |>
    # Group by single_value_cols
    dplyr::group_by(dplyr::across(dplyr::all_of(single_value_col_names))) |>
    # Nest
    tidyr::nest(.key = .nested_col) |>
    # Calculate hash
    dplyr::mutate(
      "{.nested_hash_col}" := digest::digest(.data[[.nested_col]], algo = .algo),
      "{.nested_col}" := NULL
    ) |>
    dplyr::ungroup()
}
