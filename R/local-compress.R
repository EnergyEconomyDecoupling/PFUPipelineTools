#' A local table compression helper function
#'
#' We decide before uploading to the remote database
#' whether rows in the remote table can be compressed
#' via the use of `ValidFromVersion` and `ValidToVersion` columns.
#' This function contains the logic for that work.
#'
#' In the context of this function,
#' `remote` means (sometimes older) data from the remote database.
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
#' [PFUPipelineTools::dataset_info]`$replace_valid_to_version_in_remote`,
#' [PFUPipelineTools::dataset_info]`$replace_value_in_remote`,
#' [PFUPipelineTools::dataset_info]`$delete_row_in_remote`, and
#' [PFUPipelineTools::dataset_info]`$upload_new`
#' that indicate whether to change the remote table's
#' `ValidToVersion` value,
#' replace the value in the `value` column,
#' delete the remote row, or
#' upload a new row, respectively.
#' The values are
#' "`r PFUPipelineTools::dataset_info$replace_valid_to_version_in_remote`",
#' "`r PFUPipelineTools::dataset_info$replace_value_in_remote`",
#' "`r PFUPipelineTools::dataset_info$delete_row_in_remote`", and
#' "`r PFUPipelineTools::dataset_info$upload_new`",
#' respectively.
#'
#' Functions that call [compress_helper()] should query the value
#' of the `what_to_do_colname` to decide how to handle
#' each row.
#'
#' If both `remote_df` and `local_df` are `NULL`, `NULL` is returned.
#' If only `remote_df` is `NULL`, all rows in `local_df` are assumed to require
#' uploading to the remote.
#' If only `local_df` is `NULL`, no rows need to be uploaded to the remote and
#' no rows in the remote need to be changed.
#'
#' Note that both `remote_df` and `local_df` should be
#' encoded data frames, i.e.
#' their foreign key columns should all be ID integers.
#'
#' Note: `local_df` should have the same values in both
#' `valid_from_version_colname` and
#' `valid_to_version_colname`
#' to clearly indicate intent by the caller.
#' If not, an error is thrown.
#'
#' Important: `local_df` is assumed to contain
#' a complete set of information for metadata columns
#' (columns excluding `i`, `j`, and `value`).
#' Thus, if for the same metadata and the current version,
#' `local_df` is lacking some rows present in `remote_df`,
#' those rows will be removed from `remote_df`.
#'
#' `value_colname` can be vector of length greater than 1,
#' indicating multiple value columns.
#' To accommodate this possibility,
#' `remote_df` and `local_df` are pivoted longer internally
#' to create `nam` and `val` columns.
#' If any value in a row of `local_df` is different from `remote_df`,
#' the entire row is marked as being updated.
#'
#' @param remote_df A remote version of the rows contained in `local_df`.
#' @param local_df A new data frame computed locally that
#'                 contains a new set of values for `remote_df`.
#' @param valid_from_version_colname,valid_to_version_colname See
#'            [PFUPipelineTools::dataset_info].
#'            Defaults are [PFUPipelineTools::dataset_info]`$valid_from_version_colname` and [PFUPipelineTools::dataset_info]`$valid_to_version_colname` or
#'            "`r PFUPipelineTools::dataset_info$valid_from_version_colname`" and
#'            "`r PFUPipelineTools::dataset_info$valid_to_version_colname`".
#' @param value_colname The name of the value column.
#'                      Default is [PFUPipelineTools::mat_colnames]`$value` or
#'                      "`r PFUPipelineTools::mat_colnames$value`".
#' @param nam The name of a column of names for values in the remote and local data frames.
#'            This name is used internally.
#'            Default is "nam".
#' @param val The name of a column of values in the remote and local data frames.
#'            This name is used internally.
#'            Default is "val".
#' @param what_to_do_colname The name of the column that tells
#'                           what to do with the row.
#'                           Default is [PFUPipelineTools::dataset_info]`$what_to_do` or
#'                           "`r PFUPipelineTools::dataset_info$what_to_do`".
#' @param change_valid_to_version_in_remote The string that
#'              indicates the value in the remote's
#'              `valid_to_version` column
#'              should be changed.
#'              Default is [PFUPipelineTools::dataset_info]`$replace_valid_to_version_in_remote` or
#'              "`r PFUPipelineTools::dataset_info$replace_valid_to_version_in_remote`".
#' @param replace_value_in_remote The string that
#'              indicates the value of remote's `value` column
#'              should be changed.
#'              Default is [PFUPipelineTools::dataset_info]`$replace_value_in_remote` or
#'              "`r PFUPipelineTools::dataset_info$replace_value_in_remote`".
#' @param delete_row_in_remote The string that indicates the row should be
#'              deleted from the remote database.
#'              Default is [PFUPipelineTools::dataset_info]`$delete_row_in_remote` or
#'              "`r PFUPipelineTools::dataset_info$delete_row_in_remote`".
#' @param upload_new The string that
#'              indicates local rows
#'              to be uploaded to the remote.
#'              Default is [PFUPipelineTools::dataset_info]`$upload_new` or
#'              "`r PFUPipelineTools::dataset_info$upload_new`".
#' @param current_version_int The integer representing the current version.
#'                            Default is
#'                            [PFUPipelineTools::version_info]`$current_version_int` or
#'                            `r PFUPipelineTools::version_info$current_version_int`.
#' @param tol The tolerance within which a local value will be
#'            assumed same as the remote value.
#'            Default is `1e-6`.
#'
#' @returns A data frame with same columns as `remote_df` and `local_df` and an
#'          added column (`what_to_do_colname`).
#'
#' @export
#'
#' @examples
#' remote_df <- tibble::tribble(
#'   ~Dataset, ~ValidFromVersion, ~ValidToVersion, ~Country, ~Year, ~matname, ~i, ~j, ~value,
#'    5, 2, PFUPipelineTools::version_info$current_version_int, 49, 1971, 2, 1, 1, 11,
#'    5, 2, PFUPipelineTools::version_info$current_version_int, 49, 1971, 2, 1, 2, 12,
#'    5, 2, PFUPipelineTools::version_info$current_version_int, 49, 1971, 2, 1, 3, 13,
#'    5, 2, PFUPipelineTools::version_info$current_version_int, 49, 1971, 2, 2, 1, 21,
#'    # matname = 3 is U
#'    5, 2, PFUPipelineTools::version_info$current_version_int, 49, 1971, 3, 1, 1, 110,
#'    5, 2, PFUPipelineTools::version_info$current_version_int, 49, 1971, 3, 2, 2, 220,
#'    5, 2, PFUPipelineTools::version_info$current_version_int, 49, 1971, 3, 3, 2, 320,
#'    # Country = 146 is USA
#'    # matname = 7 is V
#'    5, 2, PFUPipelineTools::version_info$current_version_int, 146, 1972, 7, 1, 1, 1100,
#'    5, 2, PFUPipelineTools::version_info$current_version_int, 146, 1972, 7, 2, 2, 2200,
#'    5, 2, PFUPipelineTools::version_info$current_version_int, 146, 1972, 7, 3, 5, 3500,
#'    # matname = 8 is Y
#'    5, 2, PFUPipelineTools::version_info$current_version_int, 146, 1972, 8, 1, 1, 11000,
#'    5, 2, PFUPipelineTools::version_info$current_version_int, 146, 1972, 8, 2, 1, 21000,
#'    5, 2, PFUPipelineTools::version_info$current_version_int, 146, 1972, 8, 1, 2, 12000
#' )
#' # Change 2 rows
#' local_df <- remote_df |>
#'   dplyr::mutate(
#'     # Change the version information
#'     "{PFUPipelineTools::dataset_info$valid_from_version_colname}" := 3,
#'     "{PFUPipelineTools::dataset_info$valid_to_version_colname}" := 3,
#'   )
#' local_df[3, PFUPipelineTools::mat_colnames$value] <- -13
#' local_df[12, PFUPipelineTools::mat_colnames$value] <- -21000
#' # Returns only the rows that need to be changed
#' # and what must be done.
#' compress_helper(remote_df = remote_df, local_df = local_df)
compress_helper <- function(remote_df, local_df,
                            valid_from_version_colname =
                              PFUPipelineTools::dataset_info$valid_from_version_colname,
                            valid_to_version_colname =
                              PFUPipelineTools::dataset_info$valid_to_version_colname,
                            value_colname = PFUPipelineTools::mat_colnames$value,
                            nam = "nam",
                            val = "val",
                            what_to_do_colname = PFUPipelineTools::dataset_info$what_to_do,
                            change_valid_to_version_in_remote =
                              PFUPipelineTools::dataset_info$replace_valid_to_version_in_remote,
                            delete_row_in_remote =
                              PFUPipelineTools::dataset_info$delete_row_in_remote,
                            replace_value_in_remote =
                              PFUPipelineTools::dataset_info$replace_value_in_remote,
                            upload_new = PFUPipelineTools::dataset_info$upload_new,
                            current_version_int = PFUPipelineTools::version_info$current_version_int,
                            tol = 1e-6) {

  if (is.null(remote_df) & is.null(local_df)) {
    return(NULL)
  }

  # If we have NULL for local_df, nothing should change.
  if (is.null(local_df)) {
    return(remote_df |>
             dplyr::mutate(
               "{what_to_do_colname}" := "bogus"
             ) |>
             # We want no rows in this data frame
             dplyr::filter(FALSE)
    )
  }

  # If local_df has no rows, we can't do anything,
  # because there is no guidance on what is to be done.
  if (nrow(local_df) == 0) {
    return(remote_df |>
             dplyr::mutate(
               "{what_to_do_colname}" := "bogus"
             ) |>
             # We want no rows in this data frame
             dplyr::filter(FALSE)
    )
  }

  # If remote_df is NULL, all rows of local_df should be uploaded.
  if (is.null(remote_df)) {
    return(local_df |>
             dplyr::mutate(
               "{valid_to_version_colname}" := current_version_int,
               "{what_to_do_colname}" := upload_new
             )
    )
  }

  # Establish some names
  remote <- "Remote"
  local <- "Local"
  diff <- "Diff"
  new_remote_from_name <- paste0(valid_from_version_colname, remote)
  new_local_from_name <- paste0(valid_from_version_colname, local)
  new_remote_to_name <- paste0(valid_to_version_colname, remote)
  new_local_to_name <- paste0(valid_to_version_colname, local)
  # new_remote_value_name <- paste0(value_colname, remote)
  # new_local_value_name <- paste0(value_colname, local)
  # value_diff_name <- paste0(value_colname, diff)
  new_remote_value_name <- paste0(val, remote)
  new_local_value_name <- paste0(val, local)
  value_diff_name <- paste0(val, diff)

  # Decide the local version and check validity
  local_version <- local_df[[valid_to_version_colname]] |>
    unique()
  assertthat::assert_that(length(local_version) == 1)
  # valid_from_version and valid_to_version should be same in local_df.
  # Make sure that's true.
  local_version_check <- local_df[[valid_from_version_colname]] |>
    unique()
  assertthat::assert_that(local_version == local_version_check)

  # Filter remote_df to contain only the current rows.
  # We want to compare local_df to only current rows in remote.
  # We should never be adjusting or
  # comparing to non-current rows in remote_df.
  remote_df <- remote_df |>
    dplyr::filter(.data[[valid_to_version_colname]] == current_version_int)

  # If remote_df has no rows, all rows of local_df are new
  # and should be uploaded.
  if (nrow(remote_df) == 0) {
    out <- local_df |>
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
    return(out)
  }

  # If we get here, both remote_df and local_df have a non-zero
  # number of rows.
  # Ensure that column names in both data frames are same.
  # If not, almost certainly an error.
  assertthat::assert_that(setequal(colnames(remote_df), colnames(local_df)))

  # Decide the previous version
  # Set the previous version relative to local_version.
  previous_version <- local_version - 1


  # We can have more than one value column.
  # Pivot the data frames to put all the values in one column.
  remote_df_long <- remote_df |>
    tidyr::pivot_longer(cols = value_colname,
                        names_to = nam,
                        values_to = val)
  local_df_long <- local_df |>
    tidyr::pivot_longer(cols = value_colname,
                        names_to = nam,
                        values_to = val)

  # Replace version column names prior to joining
  # remote_df_new_names <- remote_df |>
  #   dplyr::rename(
  #     "{new_remote_from_name}" := dplyr::all_of(valid_from_version_colname),
  #     "{new_remote_to_name}" := dplyr::all_of(valid_to_version_colname),
  #     "{new_remote_value_name}" := dplyr::all_of(value_colname)
  #   )
  remote_df_new_names <- remote_df_long |>
    dplyr::rename(
      "{new_remote_from_name}" := dplyr::all_of(valid_from_version_colname),
      "{new_remote_to_name}" := dplyr::all_of(valid_to_version_colname),
      "{new_remote_value_name}" := dplyr::all_of(val)
    )
  # local_df_new_names <- local_df |>
  #   dplyr::rename(
  #     "{new_local_from_name}" := dplyr::all_of(valid_from_version_colname),
  #     "{new_local_to_name}" := dplyr::all_of(valid_to_version_colname),
  #     "{new_local_value_name}" := dplyr::all_of(value_colname)
  #   )
  local_df_new_names <- local_df_long |>
    dplyr::rename(
      "{new_local_from_name}" := dplyr::all_of(valid_from_version_colname),
      "{new_local_to_name}" := dplyr::all_of(valid_to_version_colname),
      "{new_local_value_name}" := dplyr::all_of(val)
    )


  # Figure out columns by which to join.
  # We want to join by all columns EXCEPT
  # the new names of the
  # valid_from_version, valid_to_version, and value columns.
  # This approach uses all metadata columns for joining.
  join_cols <- c(colnames(remote_df_new_names), colnames(local_df_new_names)) |>
    setdiff(c(new_remote_from_name, new_remote_to_name, new_remote_value_name,
              new_local_from_name, new_local_to_name, new_local_value_name)) |>
    unique()
  # Perform a full join
  joined <- dplyr::full_join(remote_df_new_names,
                             local_df_new_names,
                             by = join_cols) |>
    dplyr::mutate(
      # Difference the values
      "{value_diff_name}" := .data[[new_local_value_name]] - .data[[new_remote_value_name]]
    )

  # Check that local_df is not younger than remote_df
  local_df_older <- joined |>
    dplyr::filter(.data[[new_remote_from_name]] >
                    .data[[new_local_from_name]])
  if (nrow(local_df_older) > 0) {
    stop("local_df contains older versions than remote_df in compress_helper()")
  }

  # Find all rows where the remote and local values both exist
  # and are same within tol.
  # Actually, don't need to do this, as these rows
  # do not need to be changed in the remote,
  # as they already are valid from
  # whatever version to current_version_int.
  # Their ValidToVersion column already contains current_version_int.
  # equal_rows <- joined |>
  #   dplyr::filter(abs(.data[[value_diff_name]]) <= tol) |>
  #   dplyr::mutate(
  #     # Remove the diff column
  #     "{value_diff_name}" := NULL
  #   )

  # Create an empty outgoing data frame
  # with the same columns names as the remote
  out <- remote_df[0, ]

  # Look for cases where local_df contains new data altogether, i.e.
  # a new combination of values in metadata columns.
  # In this case, the joined table will have
  # valueRemote NA
  # and
  # valueLocal not NA.
  # For this circumstance, the local rows should be uploaded
  # directly.
  # Nothing needs to be changed in existing rows of remote_df.
  completely_new_data_in_local_df <- joined |>
    dplyr::filter(is.na(.data[[new_remote_value_name]]) &
                    !is.na(.data[[new_local_value_name]]))
  if (nrow(completely_new_data_in_local_df) > 0) {
    add_to_out <- completely_new_data_in_local_df |>
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
                      current_version_int)
    out <- out |>
      dplyr::bind_rows(add_to_out)
  }

  # Look for cases where local_df lacks data that is present in remote_df.
  # These are cases where we should delete the remote row.
  # Remembering that we already filtered on current_version_int,
  # this condition will occur only if we have a new run
  # that no longer has a certain combination of metadata.
  # In this case, the joined table will have
  # valueRemote not NA
  # and
  # valueLocal NA.
  # For this circumstance, we adjust the ValidToVersion column
  # in the remote database or delete it altogether,
  # depending on whether ValidFromVersion in remote_df is same as
  # local_version or not.
  delete_rows_in_remote_df <- joined |>
    dplyr::filter(is.na(.data[[new_local_value_name]] & !is.na(.data[[new_remote_value_name]])))
  if (nrow(delete_rows_in_remote_df)) {
    # Check for rows where remote_df has same ValidFromVersion as local_version.
    # In this case, we need to delete the row from remote.
    add_to_out <- delete_rows_in_remote_df |>
      dplyr::filter(.data[[new_remote_from_name]] == local_version) |>
      prep_delete_row_in_remote(new_local_from_name = new_local_from_name,
                                new_local_to_name = new_local_to_name,
                                new_local_value_name = new_local_value_name,
                                value_diff_name = value_diff_name,
                                valid_from_version_colname = valid_from_version_colname,
                                valid_to_version_colname = valid_to_version_colname,
                                value_colname = value_colname,
                                new_remote_from_name = new_remote_from_name,
                                new_remote_to_name = new_remote_to_name,
                                new_remote_value_name = new_remote_value_name,
                                what_to_do_colname = what_to_do_colname,
                                delete_row_in_remote = delete_row_in_remote)
    out <- out |>
      dplyr::bind_rows(add_to_out)

    # Check for rows where remote_df has older ValidFromVersion than local_version.
    # In this case, we need to set ValidToVersion to local_version - 1.
    add_to_out <- delete_rows_in_remote_df |>
      dplyr::filter(.data[[new_remote_from_name]] < local_version) |>
      prep_unequal_change_valid_to_version_in_remote(
        value_diff_name = value_diff_name,
        new_local_from_name = new_local_from_name,
        new_local_to_name = new_local_to_name,
        new_local_value_name = new_local_value_name,
        valid_from_version_colname = valid_from_version_colname,
        valid_to_version_colname = valid_to_version_colname,
        value_colname = value_colname,
        new_remote_from_name = new_remote_from_name,
        new_remote_to_name = new_remote_to_name,
        new_remote_value_name = new_remote_value_name,
        what_to_do_colname = what_to_do_colname,
        change_remote = change_valid_to_version_in_remote,
        previous_version = previous_version)
    out <- out |>
      dplyr::bind_rows(add_to_out)
  }

  # Find all rows where
  # (a) the remote and local values both exist and
  # (b) the remote and local values differ more than tol
  unequal_vals <- joined |>
    dplyr::filter(abs(.data[[value_diff_name]]) > tol)

  # What to do with these rows depends on the version information.

  # If the local_version is same as the remote's valid_from_version,
  # we need to simply update the value in the remote.
  replace_remote_value <- unequal_vals |>
    dplyr::filter(.data[[new_remote_from_name]] == local_version) |>
    prep_replace_value_in_remote(new_remote_value_name = new_remote_value_name,
                                 new_local_from_name = new_local_from_name,
                                 new_local_to_name = new_local_to_name,
                                 value_diff_name = value_diff_name,
                                 valid_from_version_colname = valid_from_version_colname,
                                 new_remote_from_name = new_remote_from_name,
                                 valid_to_version_colname = valid_to_version_colname,
                                 new_remote_to_name = new_remote_to_name,
                                 value_colname = value_colname,
                                 new_local_value_name = new_local_value_name,
                                 what_to_do_colname = what_to_do_colname,
                                 replace_value_in_remote = replace_value_in_remote)
  out <- out |>
    dplyr::bind_rows(replace_remote_value)

  # If local_version is greater than the remote's valid_from_version,
  # we need to
  # (a) set remote's valid_to_version to
  #     one less than local_version
  # and
  # (b) upload local_df as new information.

  new_version <- unequal_vals |>
    dplyr::filter(.data[[new_remote_from_name]] <
                    .data[[new_local_from_name]])

  if (nrow(new_version) > 0) {
    # Find rows where we need to change the valid_to_version
    # in the remote
    new_version_change_valid_to_version_in_remote <- new_version |>
      prep_unequal_change_valid_to_version_in_remote(
        value_diff_name = value_diff_name,
        new_local_from_name = new_local_from_name,
        new_local_to_name = new_local_to_name,
        new_local_value_name = new_local_value_name,
        valid_from_version_colname = valid_from_version_colname,
        valid_to_version_colname = valid_to_version_colname,
        value_colname = value_colname,
        new_remote_from_name = new_remote_from_name,
        new_remote_to_name = new_remote_to_name,
        new_remote_value_name = new_remote_value_name,
        what_to_do_colname = what_to_do_colname,
        change_remote = change_valid_to_version_in_remote,
        previous_version = previous_version)
    out <- out |>
      dplyr::bind_rows(new_version_change_valid_to_version_in_remote)

    # Prepare rows to upload as a new version
    new_version_upload_new <- new_version |>
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
      dplyr::bind_rows(new_version_upload_new)
  }

  return(out)
}


prep_unequal_change_valid_to_version_in_remote <- function(.df,
                                                           value_diff_name,
                                                           new_local_from_name,
                                                           new_local_to_name,
                                                           new_local_value_name,
                                                           valid_from_version_colname,
                                                           valid_to_version_colname,
                                                           value_colname,
                                                           new_remote_from_name,
                                                           new_remote_to_name,
                                                           new_remote_value_name,
                                                           what_to_do_colname,
                                                           change_remote,
                                                           previous_version) {
  .df |>
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
      "{valid_from_version_colname}" := dplyr::any_of(new_remote_from_name),
      "{valid_to_version_colname}" := dplyr::any_of(new_remote_to_name),
      "{value_colname}" := dplyr::any_of(new_remote_value_name)
    ) |>
    dplyr::mutate(
      # Set the what to do column name
      "{what_to_do_colname}" := change_remote,
      # Set the value of the ValidToVersion column
      # to the previous version.
      "{valid_to_version_colname}" := previous_version
    )
}


prep_replace_value_in_remote <- function(.df,
                                         new_remote_value_name,
                                         new_local_from_name,
                                         new_local_to_name,
                                         value_diff_name,
                                         valid_from_version_colname,
                                         new_remote_from_name,
                                         valid_to_version_colname,
                                         new_remote_to_name,
                                         value_colname,
                                         new_local_value_name,
                                         what_to_do_colname,
                                         replace_value_in_remote) {
  .df |>
    dplyr::mutate(
      "{new_remote_value_name}" := NULL,
      "{new_local_from_name}" := NULL,
      "{new_local_to_name}" := NULL,
      "{value_diff_name}" := NULL
    ) |>
    dplyr::rename(
      "{valid_from_version_colname}" := dplyr::all_of(new_remote_from_name),
      "{valid_to_version_colname}" := dplyr::all_of(new_remote_to_name),
      "{value_colname}" := dplyr::all_of(new_local_value_name)
    ) |>
    dplyr::mutate(
      # Set the what to do column name
      "{what_to_do_colname}" := replace_value_in_remote,
    )
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
      "{valid_from_version_colname}" := dplyr::any_of(new_local_from_name),
      "{valid_to_version_colname}" := dplyr::any_of(new_local_to_name),
      "{value_colname}" := dplyr::any_of(new_local_value_name)
    ) |>
    dplyr::mutate(
      # Set the what to do column name
      "{what_to_do_colname}" := upload_new,
      # Set the value of the ValidToVersion column
      # to the current version
      "{valid_to_version_colname}" := current_version_int
    )
}


prep_delete_row_in_remote <- function(.df,
                                      new_local_from_name,
                                      new_local_to_name,
                                      new_local_value_name,
                                      value_diff_name,
                                      valid_from_version_colname,
                                      valid_to_version_colname,
                                      value_colname,
                                      new_remote_from_name,
                                      new_remote_to_name,
                                      new_remote_value_name,
                                      what_to_do_colname,
                                      delete_row_in_remote) {
  .df |>
    dplyr::mutate(
      "{new_local_from_name}" := NULL,
      "{new_local_to_name}" := NULL,
      "{new_local_value_name}" := NULL,
      "{value_diff_name}" := NULL
    ) |>
    dplyr::rename(
      "{valid_from_version_colname}" := dplyr::any_of(new_remote_from_name),
      "{valid_to_version_colname}" := dplyr::any_of(new_remote_to_name),
      "{value_colname}" := dplyr::any_of(new_remote_value_name)
    ) |>
    dplyr::mutate(
      "{what_to_do_colname}" := delete_row_in_remote
    )

}

