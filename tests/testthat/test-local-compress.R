test_that("local_compress() works as expected", {
  # Set up test data frames
  remote_df <- tibble::tribble(
    ~Dataset, ~ValidFromVersion, ~ValidToVersion, ~Country, ~Year, ~matname, ~i, ~j, ~value,
    # Dataset = 5 is CL-PFU IEA
    # Country = 49 is GHA
    # matname = 2 is R
    5, 2, current_version_int, 49, 1971, 2, 1, 1, 11,
    5, 2, current_version_int, 49, 1971, 2, 1, 2, 12,
    5, 2, current_version_int, 49, 1971, 2, 1, 3, 13,
    5, 2, current_version_int, 49, 1971, 2, 2, 1, 21,
    # matname = 3 is U
    5, 2, current_version_int, 49, 1971, 3, 1, 1, 110,
    5, 2, current_version_int, 49, 1971, 3, 2, 2, 220,
    5, 2, current_version_int, 49, 1971, 3, 3, 2, 320,
    # Country = 146 is USA
    # matname = 7 is V
    5, 2, current_version_int, 146, 1972, 7, 1, 1, 1100,
    5, 2, current_version_int, 146, 1972, 7, 2, 2, 2200,
    5, 2, current_version_int, 146, 1972, 7, 3, 5, 3500,
    # matname = 8 is Y
    5, 2, current_version_int, 146, 1972, 8, 1, 1, 11000,
    5, 2, current_version_int, 146, 1972, 8, 2, 1, 21000,
    5, 2, current_version_int, 146, 1972, 8, 1, 2, 12000
  )

  # Everything same as new_df but with older
  # ValidVersionFrom and ValidVersionTo equal to
  # current_version_int
  local_df <- remote_df |>
    dplyr::mutate(ValidFromVersion = 3,
                  ValidToVersion = 3)

  # There should be no rows in this one.
  # Everything is same, so no need to make changes
  no_rows <- compress_helper(remote_df = remote_df, local_df = local_df)
  expect_equal(nrow(no_rows), 0)

  # Try with one new value (change to negative value)
  local_df_one_new_value <- local_df
  local_df_one_new_value[3, PFUPipelineTools::mat_colnames$value] <- -13
  changed_1_row <- compress_helper(remote_df = remote_df,
                                   local_df = local_df_one_new_value)
  # We should have 2 rows here,
  # one to change the remote, one to upload new
  expect_equal(nrow(changed_1_row), 2)
  # Check we got the right information
  change_remote <- changed_1_row |>
    dplyr::filter(.data[[PFUPipelineTools::dataset_info$what_to_do]] == PFUPipelineTools::dataset_info$change_remote)
  # The value should still be the original value (13)
  change_remote |>
    magrittr::extract2(PFUPipelineTools::mat_colnames$value) |>
    magrittr::extract2(1) |>
    expect_equal(13)
  # Leave ValidFromVersion at 2
  change_remote |>
    magrittr::extract2(PFUPipelineTools::dataset_info$valid_from_version_colname) |>
    magrittr::extract2(1) |>
    expect_equal(2)
  # But ValidToVersion should be reset from current_version_int (huge number)
  # to 2 (one less than the version columns in the local_df)
  change_remote |>
    magrittr::extract2(PFUPipelineTools::dataset_info$valid_to_version_colname) |>
    magrittr::extract2(1) |>
    expect_equal(2)
  # Now look at the upload_new row
  upload_new <- changed_1_row |>
    dplyr::filter(.data[[PFUPipelineTools::dataset_info$what_to_do]] == PFUPipelineTools::dataset_info$upload_new)
  # The value should be the changed value (-13)
  upload_new |>
    magrittr::extract2(PFUPipelineTools::mat_colnames$value) |>
    magrittr::extract2(1) |>
    expect_equal(-13)
  # The ValidFromVersion column should be 3 (the new version)
  upload_new |>
    magrittr::extract2(PFUPipelineTools::dataset_info$valid_from_version_colname) |>
    magrittr::extract2(1) |>
    expect_equal(3)
  # The ValidToVersion column should be current_version_int,
  # a huge number
  upload_new |>
    magrittr::extract2(PFUPipelineTools::dataset_info$valid_to_version_colname) |>
    magrittr::extract2(1) |>
    expect_equal(current_version_int)

  # Test when 2 rows change
  local_df_two_new_values <- local_df
  local_df_two_new_values[3, PFUPipelineTools::mat_colnames$value] <- -13
  local_df_two_new_values[12, PFUPipelineTools::mat_colnames$value] <- -21000
  changed_2_rows <- compress_helper(remote_df = remote_df,
                                    local_df = local_df_two_new_values)
  expected_changed_2_rows <- tibble::tribble(
    ~Dataset, ~ValidFromVersion, ~ValidToVersion, ~Country, ~Year, ~matname, ~i, ~j, ~value, ~WhatToDo,
    5, 2, 2, 49, 1971, 2, 1, 3, 13, "Change ValidToVersion in remote",
    5, 2, 2, 146, 1972, 8, 2, 1, 21000, "Change ValidToVersion in remote",
    5, 3, current_version_int, 49, 1971, 2, 1, 3, -13, "Upload new",
    5, 3, current_version_int, 146, 1972, 8, 2, 1, -21000, "Upload new"
  )
  expect_equal(changed_2_rows, expected_changed_2_rows)

  # Test when all rows change
  local_df_all_new_values <- local_df |>
    dplyr::mutate(
      "{PFUPipelineTools::mat_colnames$value}" := -.data[[PFUPipelineTools::mat_colnames$value]]
    )
  expected_all_new_values <- dplyr::bind_rows(
    remote_df |>
      dplyr::mutate(
        "{PFUPipelineTools::dataset_info$valid_to_version}" := 2,
        "{PFUPipelineTools::dataset_info$what_to_do}" := PFUPipelineTools::dataset_info$change_remote
      ),
    local_df |>
      dplyr::mutate(
        "{PFUPipelineTools::mat_colnames$value}" := -.data[[PFUPipelineTools::mat_colnames$value]],
        "{PFUPipelineTools::dataset_info$valid_to_version}" := 3,
        "{PFUPipelineTools::dataset_info$what_to_do}" := PFUPipelineTools::dataset_info$upload_new
      )
  )
  changed_all_rows <- compress_helper(remote_df = remote_df,
                                      local_df = local_df_all_new_values)
  expect_equal(changed_all_rows, expected_all_new_values)
})


# Test when there are no rows in remote_df

# Test when remote has several old versions.
# Remote should be filtered for all rows with
# ValidToVersion == current_version_int

# Test when local has same metadata but different values,
# as when we are developing a new version.

# Test a case where names are not same for both data frames.

# Test a case where the remote contains NO rows that have
# ValidToVersion == current_version_int
