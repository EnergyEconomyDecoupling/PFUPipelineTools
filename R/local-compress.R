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
#' Very important: `remote_df` is assumed to have the same metadata
#' (non-value columns, i.e., primary keys,
#' excluding the version columns)
#' as `local_df`.
#' That assumption is valid when called from
#' [PFUPipelineTools::do_upsert_and_compress()],
#' which already performs a [dplyr::semi_join()]
#' to filter `remote_df` for matching metadata columns.
#'
#' Important: `local_df` is assumed to contain
#' a complete set of current information for metadata columns
#' (columns excluding `i`, `j`, and `value`).
#' Thus, if for the same metadata and the current version,
#' `local_df` is lacking some rows present in `remote_df`,
#' those rows will be removed from `remote_df`.
#'
#' This function doesn't change any rows in the remote database.
#' Rather, it returns a data frame with same columns as
#' `remote_df` and an additional column
#' named with the value of `what_to_do_colname`
#' (by default
#' [PFUPipelineTools::dataset_info]`$what_to_do` or
#' "`r PFUPipelineTools::dataset_info$what_to_do`") that tells
#' what must be done with each row
#' and modify the remote database appropriately.
#' The possible values of the `what_to_do_colname` are
#'
#' - [PFUPipelineTools::dataset_info]`$replace_valid_to_version_in_remote` or
#'   "`r PFUPipelineTools::dataset_info$replace_valid_to_version_in_remote`",
#' - [PFUPipelineTools::dataset_info]`$replace_value_in_remote` or
#'   "`r PFUPipelineTools::dataset_info$replace_value_in_remote`",
#' - [PFUPipelineTools::dataset_info]`$delete_row_in_remote` or
#'   "`r PFUPipelineTools::dataset_info$delete_row_in_remote`", and
#' - [PFUPipelineTools::dataset_info]`$upload_new_row` or
#'   "`r PFUPipelineTools::dataset_info$upload_new_row`".
#'
#' Respectively, these indicate
#' whether to change the remote table's
#' `ValidToVersion` value,
#' replace the value in the `value` column,
#' delete the remote row, or
#' upload a new row, respectively.
#'
#' Functions that call [compress_helper()] should query the value
#' of `what_to_do_colname` to decide how to handle
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
#' to clearly indicate the intent of the caller.
#' If not, an error is thrown.
#'
#' The `valid_to_version_colname` in `remote_df` must contain
#' `current_version_int`.
#' If not, an error is thrown.
#'
#' `value_colname` can be vector of length greater than 1,
#' indicating multiple value columns.
#' If any value in a row of `local_df` is different from `remote_df`,
#' the entire row is marked as updated.
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
#' @param changed_cols_colname The name of a column that tells which
#'                             columns have changed.
#'                             This column is added internally but removed
#'                             before returning.
#'                             Default is "ChangedCols".
#' @param what_to_do_colname The name of the column that tells
#'                           what to do with the row.
#'                           Default is [PFUPipelineTools::dataset_info]`$what_to_do` or
#'                           "`r PFUPipelineTools::dataset_info$what_to_do`".
#' @param replace_valid_to_version_in_remote The string that
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
#' @param upload_new_row The string that
#'              indicates local rows
#'              have new metadata and need
#'              to be uploaded to the remote.
#'              Default is [PFUPipelineTools::dataset_info]`$upload_new` or
#'              "`r PFUPipelineTools::dataset_info$upload_new`".
#' @param no_action The string that indicates no action is needed
#'              on this row.
#'              Default is [PFUPipelineTools::dataset_info]`no_action` or
#'              "`r PFUPipelineTools::dataset_info$no_action`".
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
                            changed_cols_colname = PFUPipelineTools::dataset_info$changed_cols_colname,
                            what_to_do_colname = PFUPipelineTools::dataset_info$what_to_do,
                            # delete_or_change_valid_to_in_remote = PFUPipelineTools::dataset_info$delete_or_change_valid_to_in_remote,
                            replace_valid_to_version_in_remote =
                              PFUPipelineTools::dataset_info$replace_valid_to_version_in_remote,
                            delete_row_in_remote =
                              PFUPipelineTools::dataset_info$delete_row_in_remote,
                            replace_value_in_remote =
                              PFUPipelineTools::dataset_info$replace_value_in_remote,
                            upload_new_row = PFUPipelineTools::dataset_info$upload_new_row,
                            no_action = PFUPipelineTools::dataset_info$no_action,
                            current_version_int = PFUPipelineTools::version_info$current_version_int,
                            tol = 1e-6) {

  # Check NULL and empty conditions of data frames.

  ## If both data frames are NULL, nothing to be done.
  ## Return NULL.
  if (is.null(remote_df) && is.null(local_df)) {
    return(NULL)
  }

  ## If remote_df is NULL or contains no rows,
  ## all rows of local_df should be uploaded.
  if (is.null(remote_df) || nrow(remote_df) == 0) {
    return(local_df |>
             dplyr::mutate(
               "{valid_to_version_colname}" := current_version_int,
               "{what_to_do_colname}" := upload_new_row
             )
    )
  }

  ## If we have NULL local_df or no rows,
  ## nothing should change in the remote.
  if (is.null(local_df) || nrow(local_df) == 0) {
    return(remote_df |>
             dplyr::mutate(
               "{what_to_do_colname}" := "bogus"
             ) |>
             # We want no rows in this data frame
             dplyr::filter(FALSE))
  }

  ## If we get here, neither remote_df nor local_df are NULL.
  ## And both have 1 or more rows.

  ## Ensure that remote_df contains only the current version
  ## in the ValidToVersion column.
  remote_df <- remote_df |>
    dplyr::filter(.data[[valid_to_version_colname]] == current_version_int)

  ## Again, if we now have no rows in remote_df,
  ## all rows of local_df should be uploaded.
  if (nrow(remote_df) == 0) {
    return(local_df |>
             dplyr::mutate(
               "{valid_to_version_colname}" := current_version_int,
               "{what_to_do_colname}" := upload_new_row
             ))
  }

  # If we get here, both remote_df and local_df have a non-zero
  # number of rows, and remote_df has only the most current version.
  # Ensure that column names in both data frames are same.
  # If not, almost certainly an error.
  assertthat::assert_that(setequal(colnames(remote_df), colnames(local_df)))

  # Decide the local version and check validity
  local_valid_from_version <- local_df[[valid_from_version_colname]] |>
    unique()
  assertthat::assert_that(length(local_valid_from_version) == 1)
  local_valid_to_version <- local_df[[valid_to_version_colname]] |>
    unique()
  assertthat::assert_that(length(local_valid_to_version) == 1)
  assertthat::assert_that(local_valid_from_version == local_valid_to_version)
  local_version <- local_valid_from_version

  # Decide the previous version
  previous_version <- local_version - 1

  # Establish some suffixes
  remote_suff <- "_remote"
  local_suff <- "_local"
  diff_suff <- "_diff"

  # Figure out columns by which to join.
  # We want to join by all columns EXCEPT
  # the new names of the
  # valid_from_version, valid_to_version, and value columns.
  # This approach uses all metadata columns for joining.
  join_cols <- c(colnames(remote_df), colnames(local_df)) |>
    setdiff(c(valid_from_version_colname, valid_to_version_colname, value_colname)) |>
    unique()

  # Perform a full join
  joined <- dplyr::full_join(remote_df, local_df,
                             by = join_cols,
                             suffix = c(remote_suff, local_suff))

  # Check that local_df is not younger than remote_df
  local_df_older <- joined |>
    dplyr::filter(.data[[paste0(valid_from_version_colname, remote_suff)]] >
                    .data[[paste0(valid_from_version_colname, local_suff)]])
  assertthat::assert_that(nrow(local_df_older) == 0,
                          msg = paste("local_df contains older versions",
                                      "than remote_df in compress_helper()"))

  # Figure out the next steps from the joined data frame.
  next_steps <- joined |>
    dplyr::rowwise() |>
    dplyr::mutate(
      # Identify which value columns differ
      "{changed_cols_colname}" := list({
        diffs <- purrr::map_lgl(value_colname, function(this_col) {
          this_r <- get(paste0(this_col, remote_suff))
          this_l <- get(paste0(this_col, local_suff))

          # Handle NA and floating point safely
          if (is.numeric(this_r) && is.numeric(this_l)) {
            !isTRUE(all.equal(this_r, this_l, tolerance = tol))
          } else {
            !(identical(this_r, this_l))
          }
        })
        value_colname[diffs]
      }),
      "{what_to_do_colname}" := dplyr::case_when(
        # New data. Upload to remote.
        is.na(.data[[paste0(valid_from_version_colname, remote_suff)]]) ~ upload_new_row,
        # These data no longer exist in local_df,
        # and we're working now on the same version of the database
        # (i.e., ValidFromVersion_remote is same as local_version).
        # In this event, we need to delete the row from remote_df.
        is.na(.data[[paste0(valid_from_version_colname, local_suff)]]) &
          .data[[paste0(valid_from_version_colname, remote_suff)]] == local_version ~ delete_row_in_remote,
        # These data no longer exist in local_df,
        # and we're working on a NEW version of the database
        # (i.e., ValidFromVersion_remote is earlier than local_version.)
        # In this event, we need to update the ValidToVersion column in remote_df.
        is.na(.data[[paste0(valid_from_version_colname, local_suff)]]) ~ replace_valid_to_version_in_remote,
        # One or more value changes
        length(.data[[changed_cols_colname]]) > 0 ~ replace_value_in_remote,
        # Anything else requires no action.
        TRUE ~ PFUPipelineTools::dataset_info$no_action
      ),
      # Eliminate changed_cols. We no longer need it.
      "{changed_cols_colname}" := NULL
    )

  # Build a data frame that can be used to
  # upload, delete, or adjust the remote database.
  out <- next_steps |>
    prep_out(local_version = local_version,
             previous_version = previous_version,
             valid_from_version_colname = valid_from_version_colname,
             valid_to_version_colname = valid_to_version_colname,
             value_colname = value_colname,
             remote_suff = remote_suff,
             local_suff = local_suff,
             out_template = remote_df[0, ],
             what_to_do_colname = what_to_do_colname,
             replace_valid_to_version_in_remote = replace_valid_to_version_in_remote,
             delete_row_in_remote = delete_row_in_remote,
             replace_value_in_remote = replace_value_in_remote,
             upload_new_row = upload_new_row,
             no_action = no_action,
             current_version_int = current_version_int)

  return(out)
}


#' Prepare an outgoing data frame for deciding what to do with new data
#'
#' This is a helper function for [compress_helper()].
#'
#' @param next_steps_df A data frame from [compress_helper()].
#' @param local_version An integer that tells the local version being developed.
#' @param previous_version An integer that tells the previous version
#'                         before the version on which we're working.
#' @param valid_from_version_colname The name of the ValidFromVersion column.
#' @param valid_to_version_colname The name of the ValidToVersion column.
#' @param value_colname The names of the value columns.
#'                      May be a vector of length greater than 1.
#' @param remote_suff The suffix for remote column names.
#' @param local_suff The suffix for local column names.
#' @param out_template A zero-row template data frame for the outgoing data frame.
#' @param what_to_do_colname The name of a column that tells what to do with each row.
#' @param replace_valid_to_version_in_remote A string that indicates a row
#'                                           should have its ValidToVersion value
#'                                           replaced in the remote table.
#' @param delete_row_in_remote A string that indicates a row should be deleted
#'                             from the remote table.
#' @param replace_value_in_remote A string that indicates values should be replaced
#'                                in the remote table.
#' @param upload_new_row A string that indicates the row is new and should be
#'                       uploaded to the remote table.
#' @param no_action A string that indicates there should be no action
#'                  taken upon this row.
#' @param current_version_int The integer that indicates a row is the current version.
#'
#' @returns A modified version of `next_steps_df` indicating
#'          what should be done for each row.
prep_out <- function(next_steps_df,
                     local_version, previous_version,
                     valid_from_version_colname,
                     valid_to_version_colname,
                     value_colname, remote_suff, local_suff,
                     out_template,
                     what_to_do_colname,
                     replace_valid_to_version_in_remote,
                     delete_row_in_remote,
                     replace_value_in_remote,
                     upload_new_row,
                     no_action,
                     current_version_int) {

  out <- out_template |>
    dplyr::mutate(
      # Add the WhatToDo column
      "{what_to_do_colname}" := character(0)
    )

  # Address cases where we have to delete a row in the remote database.
  # These cases can be tricky, because rows that need to be deleted
  # could actually simply need their ValidToVersion value updated.
  to_replace_value <- next_steps_df |>
    dplyr::filter(.data[[what_to_do_colname]] == replace_value_in_remote)

  if (nrow(to_replace_value) > 0) {
    same_version <- to_replace_value |>
      dplyr::filter(.data[[paste0(valid_from_version_colname, remote_suff)]] == local_version)

    if (nrow(same_version) > 0) {
      # For same_version, clean up the data frame
      # same_version_replace_value <- same_version |>
      #   dplyr::select(-tidyselect::all_of(
      #     c(paste0(valid_from_version_colname, local_suff),
      #       paste0(valid_to_version_colname, local_suff),
      #       paste0(value_colname, remote_suff)))
      #   ) |>
      #   # Rename remote columns to their base name
      #   # (ValidFromVersion and ValidToVersion)
      #   dplyr::rename_with(
      #     .fn = ~ sub(pattern = paste0(remote_suff, "$"),
      #                 replacement = "",
      #                 x = .x),
      #     .cols = dplyr::ends_with(remote_suff)
      #   ) |>
      #   # Rename remaining local columns to their base name
      #   # (value columns)
      #   dplyr::rename_with(
      #     .fn = ~ sub(pattern = paste0(local_suff, "$"),
      #                 replacement = "",
      #                 x = .x),
      #     .cols = dplyr::ends_with(local_suff)
      #   )


      # For same_version, clean up the data frame
      same_version_replace_value <- same_version |>
        rationalize_version_value_cols(version_suffix_to_remove = local_suff,
                                       value_suffix_to_remove = remote_suff,
                                       valid_from_version_colname = valid_from_version_colname,
                                       valid_to_version_colname = valid_to_version_colname,
                                       value_colname = value_colname,
                                       remote_suff = remote_suff,
                                       local_suff = local_suff)

      out <- out |>
        dplyr::bind_rows(same_version_replace_value)
    }

    # Address cases where we have a newer version.
    newer_version <- to_replace_value |>
      dplyr::filter(.data[[paste0(valid_from_version_colname, remote_suff)]] < local_version)

    if (nrow(newer_version) > 0) {
      # For newer_version, build instructions for both
      # updating the ValidToVersion in remote and
      # uploading new rows with the updated version.
      to_change_valid_from_version_in_remote <- newer_version |>
        dplyr::filter(.data[[paste0(valid_from_version_colname, remote_suff)]] !=
                        local_version) |>
        # # Eliminate the remote columns.
        # dplyr::select(-tidyselect::all_of(
        #   c(paste0(valid_from_version_colname, local_suff),
        #     paste0(valid_to_version_colname, local_suff),
        #     paste0(value_colname, local_suff)
        #   ))) |>
        # # Rename the remote columns to their base name
        # dplyr::rename_with(
        #   .fn = ~ sub(pattern = paste0(remote_suff, "$"),
        #               replacement = "",
        #               x = .x),
        #   .cols = dplyr::ends_with(remote_suff)
        # ) |>

        rationalize_version_value_cols(version_suffix_to_remove = local_suff,
                                       value_suffix_to_remove = local_suff,
                                       valid_from_version_colname = valid_from_version_colname,
                                       valid_to_version_colname = valid_to_version_colname,
                                       value_colname = value_colname,
                                       remote_suff = remote_suff,
                                       local_suff = local_suff) |>

        dplyr::mutate(
          # Change ValidToVersion to previous_version
          "{valid_to_version_colname}" := previous_version,
          # Change WhatToDo to change_valid_to_version_in_remote
          "{what_to_do_colname}" := replace_valid_to_version_in_remote
        )
      out <- out |>
        dplyr::bind_rows(to_change_valid_from_version_in_remote)

      # Now, we do a little trick.
      # The new values in all rows marked with replace_value_in_remote
      # should also be uploaded.
      # We can change WhatToDo from replace_value_in_remote to
      # upload_new_row so that the row will be uploaded
      # in the next section of code.
      next_steps_df <- next_steps_df |>
        dplyr::mutate(
          "{what_to_do_colname}" := dplyr::case_when(
            .data[[what_to_do_colname]] == replace_value_in_remote &
              .data[[paste0(valid_from_version_colname, remote_suff)]] < local_version ~ upload_new_row,
            TRUE ~ .data[[what_to_do_colname]]
          )
        )
    }
  }

  # Address cases where we need to upload new rows.
  to_upload <- next_steps_df |>
    dplyr::filter(.data[[what_to_do_colname]] == upload_new_row)
  if (nrow(to_upload) > 0) {
    # to_upload <- to_upload |>
    #   # Eliminate the remote columns.
    #   dplyr::select(-tidyselect::all_of(
    #     c(paste0(valid_from_version_colname, remote_suff),
    #       paste0(valid_to_version_colname, remote_suff),
    #       paste0(value_colname, remote_suff)
    #     ))) |>
    #   # Rename the local columns to their base name
    #   dplyr::rename_with(
    #     .fn = ~ sub(pattern = paste0(local_suff, "$"),
    #                 replacement = "",
    #                 x = .x),
    #     .cols = dplyr::ends_with(local_suff)
    #   ) |>
    #   dplyr::mutate(
    #     "{valid_to_version_colname}" := current_version_int
    #   )

    to_upload <- to_upload |>
      rationalize_version_value_cols(version_suffix_to_remove = remote_suff,
                                     value_suffix_to_remove = remote_suff,
                                     valid_from_version_colname = valid_from_version_colname,
                                     valid_to_version_colname = valid_to_version_colname,
                                     value_colname = value_colname,
                                     remote_suff = remote_suff,
                                     local_suff = local_suff) |>
        dplyr::mutate(
          "{valid_to_version_colname}" := current_version_int
        )

    out <- out |>
      dplyr::bind_rows(to_upload)
  }

  # Address cases where we need to delete a row in the remote.
  delete_remote <- next_steps_df |>
    dplyr::filter(.data[[what_to_do_colname]] == delete_row_in_remote)
  if (nrow(delete_remote) > 0) {
    # to_delete_remote <- delete_remote |>
    #   # Eliminate the local columns
    #   dplyr::select(-tidyselect::all_of(
    #     c(paste0(valid_from_version_colname, local_suff),
    #       paste0(valid_to_version_colname, local_suff),
    #       paste0(value_colname, local_suff)
    #     ))) |>
    #   # Rename the remote columns to their base name
    #   dplyr::rename_with(
    #     .fn = ~ sub(pattern = paste0(remote_suff, "$"),
    #                 replacement = "",
    #                 x = .x),
    #     .cols = dplyr::ends_with(remote_suff)
    #   )
    to_delete_remote <- delete_remote |>
      rationalize_version_value_cols(version_suffix_to_remove = local_suff,
                                     value_suffix_to_remove = local_suff,
                                     valid_from_version_colname = valid_from_version_colname,
                                     valid_to_version_colname = valid_to_version_colname,
                                     value_colname = value_colname,
                                     remote_suff = remote_suff,
                                     local_suff = local_suff)
    out <- out |>
      dplyr::bind_rows(to_delete_remote)
  }

  # Address cases where we need to update the ValidToVersion column.
  replace_valid_to_version <- next_steps_df |>
    dplyr::filter(.data[[what_to_do_colname]] == replace_valid_to_version_in_remote)

  if (nrow(replace_valid_to_version)) {
    # to_replace_valid_to_version <- replace_valid_to_version |>
    #   # Eliminate the local columns
    #   dplyr::select(-tidyselect::all_of(
    #     c(paste0(valid_from_version_colname, local_suff),
    #       paste0(valid_to_version_colname, local_suff),
    #       paste0(value_colname, local_suff)
    #     ))) |>
    #   # Rename the remote columns to their base name
    #   dplyr::rename_with(
    #     .fn = ~ sub(pattern = paste0(remote_suff, "$"),
    #                 replacement = "",
    #                 x = .x),
    #     .cols = dplyr::ends_with(remote_suff)
    #   ) |>
    to_replace_valid_to_version <- replace_valid_to_version |>
      rationalize_version_value_cols(version_suffix_to_remove = local_suff,
                                     value_suffix_to_remove = local_suff,
                                     valid_from_version_colname = valid_from_version_colname,
                                     valid_to_version_colname = valid_to_version_colname,
                                     value_colname = value_colname,
                                     remote_suff = remote_suff,
                                     local_suff = local_suff) |>
      # Set the ValidToVersion column to previous_version
      dplyr::mutate(
        "{valid_to_version_colname}" := previous_version
      )
    out <- out |>
      dplyr::bind_rows(to_replace_valid_to_version)
  }

  return(out)
}


#' Adjust column titles while deciding next steps
#'
#' When deciding what to do with new or adjusted data,
#' this function adjusts title of joined version and value columns.
#' This function is used as a helper function for
#' [PFUPipelineTools::prep_out()].
#'
#' @param .df The data frame whose column names are to be adjusted.
#' @param version_suffix_to_remove The string suffix on version column names
#'                                 that are to be deleted.
#' @param value_suffix_to_remove The string suffix on value column names
#'                               that are to be deleted.
#' @param valid_from_version_colname The string name of the ValidFromVersion column.
#' @param valid_to_version_colname The string name of the ValidToVersion column.
#' @param value_colname The string name of the value columns.
#'                      May be a vector of multiple value columns.
#' @param remote_suff The remote suffix for column names.
#' @param local_suff The local suffix for column names.
#'
#' @returns A modified version of `.df` that is amenable to
#'          uploading or adjusting a remote data frame.
rationalize_version_value_cols <- function(.df,
                                           version_suffix_to_remove,
                                           value_suffix_to_remove,
                                           valid_from_version_colname,
                                           valid_to_version_colname,
                                           value_colname,
                                           remote_suff,
                                           local_suff) {
  .df |>
    # Eliminate unneeded columns
    dplyr::select(-tidyselect::all_of(
      c(paste0(valid_from_version_colname, version_suffix_to_remove),
        paste0(valid_to_version_colname, version_suffix_to_remove),
        paste0(value_colname, value_suffix_to_remove)
      ))) |>
    # Rename remaining columns to their base name
    dplyr::rename_with(
      .fn = ~ sub(
        pattern = paste0("(", remote_suff, "|", local_suff, ")$"),
        replacement = "",
        x = .x
      ),
      .cols = dplyr::ends_with(c(remote_suff, local_suff))
    )
}
