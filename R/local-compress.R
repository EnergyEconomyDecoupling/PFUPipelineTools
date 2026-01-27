#' A local table compression helper function
#'
#' We decide before uploading to the remote database
#' whether rows in the remote table can be compressed
#' via the use of `ValidFromVersion` and `ValidToVersion` columns.
#' This function contains the logic for that work.
#'
#' In the context of this function,
#' `remote` means (usually older) data from the remote database.
#' `local` means new data calculated locally and meant to be uploaded
#' to the remote database.
#'
#' This function doesn't change any rows in the remote database.
#' Rather, it returns a data frame with same columns as
#' `remote_df` and an additional column
#' named with the value of `what_to_do_colname`
#' (by default
#' [PFUPipelineTools::dataset_info]`$what_to_do` or
#' "`r PFUPipelineTools::dataset_info$what_to_do`") that tells
#' what must be done with each row.
#' The possible values of the `what_to_do_colname` are
#' [PFUPipelineTools::dataset_info]`$change_remote` and
#' [PFUPipelineTools::dataset_info]`$upload_new`
#' that indicate whether to change the remote table's
#' `ValidToVersion` value or
#' upload a new row, respectively.
#' The values are
#' "`r PFUPipelineTools::dataset_info$change_remote`" and
#' "`r PFUPipelineTools::dataset_info$upload_new`",
#' respectively.
#'
#' Functions that call [compress_helper()] should query the value
#' of the `what_to_do_colname` to decide how to handle
#' each row.
#'
#' @param remote_df A remote version of the rows contained in `local_df`.
#' @param local_df A new data frame computed locally that
#'                 contains new values for `remote_df`.
#' @param valid_from_version_colname,valid_to_version_colname See
#'            [PFUPipelineTools::dataset_info].
#'            Defaults are [PFUPipelineTools::dataset_info]`$valid_from_version_colname` and [PFUPipelineTools::dataset_info]`$valid_to_version_colname` or
#'            `r PFUPipelineTools::dataset_info$valid_from_version_colname` and
#'            `r PFUPipelineTools::dataset_info$valid_to_version_colname`
#' @param value_colname See [PFUPipelineTools::mat_colnames$value].
#' @param current_version_int The integer representing the current version.
#'                            Default is
#'                            [PFUPipelineTools::current_version_int] or
#'                            `r PFUPipelineTools::current_version_int`.
#' @param tol The tolerance within which a local value will be
#'            assumed same as the remote value.
#'            Default is `1e-6`.
#'
#' @returns A list of two data frames
#' @export
#'
#' @examples
compress_helper <- function(remote_df, local_df,
                            valid_from_version_colname = PFUPipelineTools::dataset_info$valid_from_version_colname,
                            valid_to_version_colname = PFUPipelineTools::dataset_info$valid_to_version_colname,
                            value_colname = PFUPipelineTools::mat_colnames$value,
                            what_to_do_colname = PFUPipelineTools::dataset_info$what_to_do,
                            change_remote = PFUPipelineTools::dataset_info$change_remote,
                            upload_new = PFUPipelineTools::dataset_info$upload_new,
                            current_version_int = PFUPipelineTools::current_version_int,
                            tol = 1e-6) {

  # Decide the latest and previous versions
  latest_version <- local_df[[valid_to_version_colname]] |>
    unique()
  assertthat::assert_that(length(latest_version) == 1)
  latest_version_check <- local_df[[valid_from_version_colname]] |>
    unique()
  assertthat::assert_that(latest_version == latest_version_check)
  previous_version <- latest_version - 1

  # Filter remote_df to contain only the current rows.
  # We want to compare local_df to only current rows in remote.
  remote_df <- remote_df |>
    dplyr::filter(.data[[valid_to_version_colname]] == current_version_int)


  # Ensure that names are same.
  # If not, almost certainly an error.
  assertthat::assert_that(setequal(colnames(remote_df), colnames(local_df)))

  remote <- "Remote"
  local <- "Local"
  diff <- "Diff"
  new_remote_from_name <- paste0(valid_from_version_colname, remote)
  new_remote_to_name <- paste0(valid_to_version_colname, remote)
  new_remote_value_name <- paste0(value_colname, remote)
  new_local_from_name <- paste0(valid_from_version_colname, local)
  new_local_to_name <- paste0(valid_to_version_colname, local)
  new_local_value_name <- paste0(value_colname, local)
  value_diff_name <- paste0(value_colname, diff)

  # Replace version column names prior to joining
  remote_df_new_names <- remote_df |>
    dplyr::rename(
      "{new_remote_from_name}" := dplyr::all_of(valid_from_version_colname),
      "{new_remote_to_name}" := dplyr::all_of(valid_to_version_colname),
      "{new_remote_value_name}" := dplyr::all_of(value_colname)
    )
  local_df_new_names <- local_df |>
    dplyr::rename(
      "{new_local_from_name}" := dplyr::all_of(valid_from_version_colname),
      "{new_local_to_name}" := dplyr::all_of(valid_to_version_colname),
      "{new_local_value_name}" := dplyr::all_of(value_colname)
    )
  # Figure out columns by which to join
  join_cols <- c(colnames(remote_df_new_names), colnames(local_df_new_names)) |>
    setdiff(c(new_remote_from_name, new_remote_to_name, new_remote_value_name,
              new_local_from_name, new_local_to_name, new_local_value_name))
  # Perform a full join
  joined <- dplyr::full_join(remote_df_new_names,
                             local_df_new_names,
                             by = join_cols) |>
    dplyr::mutate(
      # Difference the values
      "{value_diff_name}" := .data[[new_local_value_name]] - .data[[new_remote_value_name]]
    )

  # Note the join will rename the columns if there is inconsistency
  # in the column names of the two data frames.
  # Check for this error.
  # assertthat::assert_that(all(join_cols %in% joined))


  # Find all rows where the remote and local values both exist
  # and are same within tol.
  # Actually, don't need to do this, as these rows
  # will not be changed in the remote.
  # equal_rows <- joined |>
  #   dplyr::filter(abs(.data[[value_diff_name]]) <= tol) |>
  #   dplyr::mutate(
  #     # Remove the diff column
  #     "{value_diff_name}" := NULL
  #   )

  # Create an empty outgoing data frame
  # with the same columns names as the remote
  out <- remote_df[0, ]

  # Find all rows where the remote and local values both exist
  # and are different beyond tol.
  # For these rows, the remote needs its ValidToVersion column
  # set to the earlier version.
  # The ValidFromVersion column stays same.
  # For these rows, the ValidFromVersion column
  # in local does not need to change.
  # However, the ValidToVersion column must be set to current version.
  # Then both data frames need to be rbound to out.
  unequal_rows <- joined |>
    dplyr::filter(abs(.data[[value_diff_name]]) > tol)
  # Create pieces of the outgoing data frame
  unequal_change_remote <- unequal_rows |>
    dplyr::mutate(
      # Remove the diff and local columns
      "{value_diff_name}" := NULL,
      "{new_local_from_name}" := NULL,
      "{new_local_to_name}" := NULL,
      "{new_local_value_name}" := NULL
    ) |>
    dplyr::rename(
      # Rename the remote valid and value columns
      # back to their original names
      "{valid_from_version_colname}" := dplyr::all_of(new_remote_from_name),
      "{valid_to_version_colname}" := dplyr::all_of(new_remote_to_name),
      "{value_colname}" := dplyr::all_of(new_remote_value_name)
    ) |>
    dplyr::mutate(
      # Set the what to do column name
      "{what_to_do_colname}" := change_remote,
      # Set the value of the ValidToVersion column
      # to the previous version.
      "{valid_to_version_colname}" := previous_version
    )
  out <- out |>
    dplyr::bind_rows(unequal_change_remote)

  unequal_upload_new <- unequal_rows |>
    dplyr::mutate(
      # Remove the diff and remote columns
      "{value_diff_name}" := NULL,
      "{new_remote_from_name}" := NULL,
      "{new_remote_to_name}" := NULL,
      "{new_remote_value_name}" := NULL,
    ) |>
    dplyr::rename(
      # Rename the local valid and value columns
      # back to their original names
      "{valid_from_version_colname}" := dplyr::all_of(new_local_from_name),
      "{valid_to_version_colname}" := dplyr::all_of(new_local_to_name),
      "{value_colname}" := dplyr::all_of(new_local_value_name)
    ) |>
    dplyr::mutate(
      # Set the what to do column name
      "{what_to_do_colname}" := upload_new,
      # Set the value of the ValidFromVersion column
      # to the latest version.
      "{valid_from_version_colname}" := latest_version,
      # Set the value of the ValidToVersion column
      # to the current version
      "{valid_to_version_colname}" := current_version_int
    )
  # Add to the outgoing data frame
  out <- out |>
    dplyr::bind_rows(unequal_upload_new)

  # Find all rows where remote valid from and valid to columns are missing.
  # This indicates that the corresponding local rows contain new information.
  # All of these rows should be uploaded.
  new_local_to_upload <- joined |>
    dplyr::filter(is.na(.data[[new_remote_from_name]]) &
                    is.na(.data[[new_remote_to_name]])) |>
    prep_upload_new(value_diff_name = value_diff_name,
                    new_remote_from_name = new_remote_from_name,
                    new_remote_to_name = new_remote_to_name,
                    new_remote_value_name = new_remote_value_name,
                    valid_from_version_colname = valid_from_version_colname,
                    valid_to_version_colname = valid_to_version_colname,
                    value_colname = value_colname,
                    new_local_from_name = new_local_from_name,
                    new_local_to_name = new_local_to_name,
                    new_local_value_name = new_local_value_name,
                    what_to_do_colname = what_to_do_colname,
                    upload_new = upload_new,
                    current_version_int = current_version_int)
  out <- out |>
    dplyr::bind_rows(new_local_to_upload)

  return(out)
}



prep_upload_new <- function(.df,
                            value_diff_name,
                            new_remote_from_name,
                            new_remote_to_name,
                            new_remote_value_name,
                            valid_from_version_colname,
                            valid_to_version_colname,
                            value_colname,
                            new_local_from_name,
                            new_local_to_name,
                            new_local_value_name,
                            what_to_do_colname,
                            upload_new,
                            current_version_int) {
  .df |>
    dplyr::mutate(
      # Remove the diff and remote columns
      "{value_diff_name}" := NULL,
      "{new_remote_from_name}" := NULL,
      "{new_remote_to_name}" := NULL,
      "{new_remote_value_name}" := NULL,
    ) |>
    dplyr::rename(
      # Rename the local valid and value columns
      # back to their original names
      "{valid_from_version_colname}" := dplyr::all_of(new_local_from_name),
      "{valid_to_version_colname}" := dplyr::all_of(new_local_to_name),
      "{value_colname}" := dplyr::all_of(new_local_value_name)
    ) |>
    dplyr::mutate(
      # Set the what to do column name
      "{what_to_do_colname}" := upload_new,
      # Set the value of the ValidToVersion column
      # to the current version
      "{valid_to_version_colname}" := current_version_int
    )
}
