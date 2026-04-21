# Create a data frame for testing purposes
remote_df_func <- function() {
  current_version_int <- version_info$current_version_int
  tibble::tribble(
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
}

local_df_func <- function() {
  # Same data with updated version.
  # Modify in tests as required.
  remote_df_func() |>
    dplyr::mutate(
      "{PFUPipelineTools::dataset_info$valid_from_version_colname}" := 3,
      "{PFUPipelineTools::dataset_info$valid_to_version_colname}" := 3
    )
}


test_that("local_compress() works as expected", {
  # Set up test data frames
  remote_df <- remote_df_func()

  # Everything same as new_df but with older
  # ValidVersionFrom and ValidVersionTo equal to
  # current_version_int
  local_df <- local_df_func()

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
    dplyr::filter(.data[[PFUPipelineTools::dataset_info$what_to_do]] == PFUPipelineTools::dataset_info$replace_valid_to_version_in_remote)
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
    expect_equal(version_info$current_version_int)

  # Test when 2 rows change
  local_df_two_new_values <- local_df
  local_df_two_new_values[3, PFUPipelineTools::mat_colnames$value] <- -13
  local_df_two_new_values[12, PFUPipelineTools::mat_colnames$value] <- -21000
  changed_2_rows <- compress_helper(remote_df = remote_df,
                                    local_df = local_df_two_new_values)
  expected_changed_2_rows <- tibble::tribble(
    ~Dataset, ~ValidFromVersion, ~ValidToVersion, ~Country, ~Year, ~matname, ~i, ~j, ~value, ~WhatToDo,
    5, 2, 2, 49, 1971, 2, 1, 3, 13,
                    PFUPipelineTools::dataset_info$replace_valid_to_version_in_remote,
    5, 2, 2, 146, 1972, 8, 2, 1, 21000,
                    PFUPipelineTools::dataset_info$replace_valid_to_version_in_remote,
    5, 3, version_info$current_version_int, 49, 1971, 2, 1, 3, -13,
                    PFUPipelineTools::dataset_info$upload_new,
    5, 3, version_info$current_version_int, 146, 1972, 8, 2, 1, -21000,
                    PFUPipelineTools::dataset_info$upload_new
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
        "{PFUPipelineTools::dataset_info$what_to_do}" :=
          PFUPipelineTools::dataset_info$replace_valid_to_version_in_remote
      ),
    local_df |>
      dplyr::mutate(
        "{PFUPipelineTools::mat_colnames$value}" := -.data[[PFUPipelineTools::mat_colnames$value]],
        "{PFUPipelineTools::dataset_info$valid_to_version}" := version_info$current_version_int,
        "{PFUPipelineTools::dataset_info$what_to_do}" := PFUPipelineTools::dataset_info$upload_new
      )
  )
  changed_all_rows <- compress_helper(remote_df = remote_df,
                                      local_df = local_df_all_new_values)
  expect_equal(nrow(changed_all_rows), 2*nrow(remote_df))
  expect_equal(changed_all_rows, expected_all_new_values)
})


test_that("compress_helper() works with no rows in remote_df and local_df", {
  # Try when no rows are present in the remote
  remote_df <- remote_df_func() |>
    dplyr::filter(FALSE)
  local_df <- local_df_func()
  res_no_rows_remote <- compress_helper(remote_df = remote_df, local_df = local_df)

  expected_no_rows_remote <- local_df |>
    dplyr::mutate(
      "{PFUPipelineTools::dataset_info$valid_to_version}" := version_info$current_version_int,
      "{PFUPipelineTools::dataset_info$what_to_do}" := PFUPipelineTools::dataset_info$upload_new
    )
  expect_equal(res_no_rows_remote, expected_no_rows_remote)

  # Try when no rows are present in local
  remote_df <- remote_df_func()
  local_df <- local_df_func() |>
    dplyr::filter(FALSE)
  res_no_rows_local <- compress_helper(remote_df = remote_df, local_df = local_df)
  expect_equal(res_no_rows_local, remote_df |>
                 dplyr::mutate(
                   "{PFUPipelineTools::dataset_info$what_to_do}" := PFUPipelineTools::dataset_info$upload_new,
                 ) |>
                 dplyr::filter(FALSE))

  # Try with no rows in both
  remote_df <- remote_df_func() |>
    dplyr::filter(FALSE)
  local_df <- local_df_func() |>
    dplyr::filter(FALSE)
  res_no_rows_remote_local <- compress_helper(remote_df = remote_df, local_df = local_df)
  expect_equal(res_no_rows_remote_local, remote_df |>
                 dplyr::mutate(
                   "{PFUPipelineTools::dataset_info$what_to_do}" := PFUPipelineTools::dataset_info$upload_new,
                 ) |>
                 dplyr::filter(FALSE))
})


test_that("compress_helper() works with NULL remote_df or local_df or both", {
  # Try with NULL remote_df.
  # Should get local_df with ValidToVersion set to the big int
  remote_df <- remote_df_func()
  local_df <- local_df_func()
  res_NULL_remote <- compress_helper(remote_df = NULL,
                                     local_df = local_df)
  expected_NULL_remote <- local_df |>
    dplyr::mutate(
      "{PFUPipelineTools::dataset_info$what_to_do}" := PFUPipelineTools::dataset_info$upload_new,
      "{PFUPipelineTools::dataset_info$valid_to_version_colname}" := version_info$current_version_int
    )
  expect_equal(res_NULL_remote, expected_NULL_remote)

  # Try with NULL local_df
  res_NULL_local <- compress_helper(remote_df = remote_df,
                                    local_df = NULL)
  expected_NULL_local <- remote_df |>
    dplyr::mutate(
      "{PFUPipelineTools::dataset_info$what_to_do}" :=
        PFUPipelineTools::dataset_info$replace_valid_to_version_in_remote
    ) |>
    dplyr::filter(FALSE)
  expect_equal(res_NULL_local, expected_NULL_local)

  # Try when both remote_df and local_df are NULL
  res_NULL_both <- compress_helper(remote_df = NULL,
                                   local_df = NULL)
  expect_null(res_NULL_both)
})


test_that("compress_helper() works when local has same metadata but different values", {
  remote_df <- remote_df_func()
  local_df <- local_df_func() |>
    dplyr::mutate(
      "{PFUPipelineTools::dataset_info$valid_from_version}" := 2,
      "{PFUPipelineTools::dataset_info$valid_to_version}" := 2,
      "{PFUPipelineTools::mat_colnames$value}" := .data[[PFUPipelineTools::mat_colnames$value]] * 10
    )
  expected <- remote_df
  expected[[PFUPipelineTools::mat_colnames$value]] <-
    local_df[[PFUPipelineTools::mat_colnames$value]]
  expected <- expected |>
    dplyr::mutate(
      "{PFUPipelineTools::dataset_info$what_to_do}" := PFUPipelineTools::dataset_info$replace_value_in_remote
    )
  res <- compress_helper(remote_df = remote_df, local_df = local_df)
  expect_equal(res, expected)
})


test_that("compress_helper() works when local has same metadata but different values for only some rows", {
  remote_df <- remote_df_func()
  local_df <- local_df_func() |>
    dplyr::mutate(
      "{PFUPipelineTools::dataset_info$valid_from_version}" := 2,
      "{PFUPipelineTools::dataset_info$valid_to_version}" := 2,
      "{PFUPipelineTools::mat_colnames$value}" := .data[[PFUPipelineTools::mat_colnames$value]] * c(rep(1, 5), rep(10, 8))
    )

  expected <- remote_df[6:13,]

  expected[[PFUPipelineTools::mat_colnames$value]] <-
    local_df[[PFUPipelineTools::mat_colnames$value]][6:13]
  expected <- expected |>
    dplyr::mutate(
      "{PFUPipelineTools::dataset_info$what_to_do}" := PFUPipelineTools::dataset_info$replace_value_in_remote
    )
  res <- compress_helper(remote_df = remote_df, local_df = local_df)
  expect_equal(res, expected)
})


test_that("compress_helper() throws an error when local_df has older data than remote_df", {
  remote_df <- remote_df_func()
  local_df <- remote_df |>
    dplyr::mutate(
      "{PFUPipelineTools::dataset_info$valid_from_version}" := 1,
      "{PFUPipelineTools::dataset_info$valid_to_version}" := 1
    )
  compress_helper(remote_df = remote_df, local_df = local_df) |>
    expect_error("local_df contains older versions than remote_df in compress_helper")
})


test_that("compress_helper() works when remote has lines of old versions", {
  remote_df <- dplyr::bind_rows(
    # Add some old data (with small values)
    remote_df_func() |>
      dplyr::mutate(
        "{PFUPipelineTools::dataset_info$valid_from_version}" := 1,
        "{PFUPipelineTools::dataset_info$valid_to_version}" := 1,
        "{PFUPipelineTools::mat_colnames$value}" := .data[[PFUPipelineTools::mat_colnames$value]] / 1000
      ),
    # Add some current data
    remote_df_func()
  )
  # Generate some new local data with the same values
  local_df <- local_df_func()

  res <- compress_helper(remote_df = remote_df, local_df = local_df)

  # In this event, the current values in remote
  # can remain current, as we are using the trick that ValidToVersion
  # is a big number
  expect_equal(nrow(res), 0)

  # now try with one value changed in local_df
  local_df2 <- local_df
  local_df2[2, PFUPipelineTools::mat_colnames$value] <- 42
  res2 <- compress_helper(remote_df = remote_df, local_df = local_df2)
  expected <- remote_df[15, ] |>
    dplyr::mutate(
      "{PFUPipelineTools::dataset_info$valid_to_version}" := 2,
      "{PFUPipelineTools::dataset_info$what_to_do}" := PFUPipelineTools::dataset_info$replace_valid_to_version_in_remote
    ) |>
    dplyr::bind_rows(
      local_df[2, ] |>
        dplyr::mutate(
          "{PFUPipelineTools::dataset_info$valid_to_version}" := version_info$current_version_int,
          "{PFUPipelineTools::mat_colnames$value}" := 42,
          "{PFUPipelineTools::dataset_info$what_to_do}" := PFUPipelineTools::dataset_info$upload_new
        )
    )
  expect_equal(res2, expected)
})


test_that("compress_helper() works with completely new information with updated version", {
  remote_df <- remote_df_func()
  local_df <- local_df_func()[1, ] |>
    dplyr::mutate(
      "{PFUPipelineTools::mat_colnames$value}" := 42,
      "{PFUPipelineTools::usual_hash_group_cols[['country']]}" := 10000,
      "{PFUPipelineTools::usual_hash_group_cols[['year']]}" := 10000
    )
  res <- compress_helper(remote_df = remote_df, local_df = local_df)
  expected <- local_df |>
    dplyr::mutate(
      "{PFUPipelineTools::dataset_info$valid_to_version}" := version_info$current_version_int,
      "{PFUPipelineTools::dataset_info$what_to_do}" := PFUPipelineTools::dataset_info$upload_new
    ) |>
    dplyr::bind_rows(
      remote_df |>
        dplyr::mutate(
          "{PFUPipelineTools::dataset_info$valid_to_version}" := 2,
          "{PFUPipelineTools::dataset_info$what_to_do}" := PFUPipelineTools::dataset_info$replace_valid_to_version_in_remote
        )
      )
  expect_equal(res, expected)
})


test_that("compress_helper() works when there are new row and column names", {
  remote_df <- remote_df_func()
  local_df <- local_df_func()[2, ] |>
    dplyr::mutate(
      # By changing the integers in the i and j columns,
      # we're changing the row and column names
      "{PFUPipelineTools::mat_colnames$i}" := 1000,
      "{PFUPipelineTools::mat_colnames$j}" := 1000
    )
  res <- compress_helper(remote_df = remote_df, local_df = local_df)
  expected <- local_df |>
    dplyr::mutate(
      "{PFUPipelineTools::dataset_info$valid_to_version}" := version_info$current_version_int,
      "{PFUPipelineTools::dataset_info$what_to_do}" := PFUPipelineTools::dataset_info$upload_new
    ) |>
    dplyr::bind_rows(
      remote_df |>
        dplyr::mutate(
          "{PFUPipelineTools::dataset_info$valid_to_version}" := 2,
          "{PFUPipelineTools::dataset_info$what_to_do}" := PFUPipelineTools::dataset_info$replace_valid_to_version_in_remote
        )
    )
  expect_equal(res, expected)
})


test_that("compress_helper() works when there are no remote_df rows for the current version", {
  remote_df <- remote_df_func() |>
    dplyr::mutate(
      # Doesn't go to current version.
      # Only from version 2 and to version 2.
      "{PFUPipelineTools::dataset_info$valid_to_version}" := 2
    )
  local_df <- local_df_func()
  res <- compress_helper(remote_df = remote_df, local_df = local_df)
  # In this case, all of the local rows should be uploaded.
  expected <- local_df |>
    dplyr::mutate(
      "{PFUPipelineTools::dataset_info$valid_to_version}" := version_info$current_version_int,
      "{PFUPipelineTools::dataset_info$what_to_do}" := PFUPipelineTools::dataset_info$upload_new
    )
    expect_equal(res, expected)
})


test_that("compress_helper() correctly identifies rows that should be removed from remote_df", {
  remote_df <- remote_df_func()
  local_df <- remote_df[c(-7, -13), ] |>
    dplyr::mutate(
      "{PFUPipelineTools::dataset_info$valid_to_version_colname}" := 2
    )
  res <- compress_helper(remote_df = remote_df, local_df = local_df)
  # Check that we have 2 rows to remove from remote in res
  res |>
    dplyr::filter(.data[[PFUPipelineTools::dataset_info$what_to_do]] == PFUPipelineTools::dataset_info$delete_row_in_remote) |>
    nrow() |>
    expect_equal(2)
  expected <- remote_df[c(7, 13), ] |>
    dplyr::mutate(
      "{PFUPipelineTools::dataset_info$what_to_do}" := PFUPipelineTools::dataset_info$delete_row_in_remote
    )
  expect_equal(res, expected)
})


test_that("compress_helper() works as expected when the current version in remote_df started several versions ago", {
  remote_df <- remote_df_func()
  local_df <- remote_df_func() |>
    dplyr::mutate(
      "{PFUPipelineTools::dataset_info$valid_from_version}" :=
        .data[[PFUPipelineTools::dataset_info$valid_from_version]] + 10,
      "{PFUPipelineTools::dataset_info$valid_to_version}" :=
        .data[[PFUPipelineTools::dataset_info$valid_from_version]],
      "{PFUPipelineTools::mat_colnames$value}" := .data[[PFUPipelineTools::mat_colnames$value]] + 100
    )
  res <- compress_helper(remote_df = remote_df, local_df = local_df)
  expected <- dplyr::bind_rows(
    remote_df |>
      dplyr::mutate(
        "{PFUPipelineTools::dataset_info$valid_to_version}" := 11,
        "{PFUPipelineTools::dataset_info$what_to_do}" := PFUPipelineTools::dataset_info$replace_valid_to_version_in_remote
      ),
    local_df |>
      dplyr::mutate(
        "{PFUPipelineTools::dataset_info$valid_to_version}" := version_info$current_version_int,
        "{PFUPipelineTools::dataset_info$what_to_do}" := PFUPipelineTools::dataset_info$upload_new
      )
  )
  expect_equal(res, expected)
})


test_that("compress_helper() works as expected when the current version in remote_df started several versions ago, some data from remote_df do not appear in local_df, but later reappear", {
  remote_df <- remote_df_func()
  local_df_orig <- remote_df_func() |>
    dplyr::mutate(
      "{PFUPipelineTools::dataset_info$valid_from_version}" :=
        .data[[PFUPipelineTools::dataset_info$valid_from_version]] + 10,
      "{PFUPipelineTools::dataset_info$valid_to_version}" :=
        .data[[PFUPipelineTools::dataset_info$valid_from_version]],
      "{PFUPipelineTools::mat_colnames$value}" := .data[[PFUPipelineTools::mat_colnames$value]] + 100
    )
    # Get rid of the 3rd row in local_df.
    # So should delete it from remote_df by adjusting the ValidToVersion column
    # but not by deleting it from remote_df.
  local_df <- local_df_orig |>
    dplyr::slice(-3)
  res <- compress_helper(remote_df = remote_df, local_df = local_df)
  expected <- dplyr::bind_rows(
    remote_df |>
      dplyr::slice(3) |>
      dplyr::mutate(
        "{PFUPipelineTools::dataset_info$valid_to_version}" := 11
      ),
    remote_df |>
      dplyr::slice(-3) |>
      dplyr::mutate(
        "{PFUPipelineTools::dataset_info$valid_to_version}" := 11
      )) |>
    dplyr::mutate(
      "{PFUPipelineTools::dataset_info$what_to_do}" := PFUPipelineTools::dataset_info$replace_valid_to_version_in_remote
    ) |>
    dplyr::bind_rows(
      local_df |>
        dplyr::mutate(
          "{PFUPipelineTools::dataset_info$valid_to_version}" := version_info$current_version_int,
          "{PFUPipelineTools::dataset_info$what_to_do}" := PFUPipelineTools::dataset_info$upload_new
        )
    )
  expect_equal(res, expected)

  # Now, re-add the deleted row in local_df and try again.
  # In this scenario, remote_df will be different
  remote_df2 <- res |>
    dplyr::mutate(
      "{PFUPipelineTools::dataset_info$what_to_do}" := NULL
    )
  local_df2 <- local_df_orig
  # Set back to original value
  local_df2[3, "value"] <- 13
  res2 <- compress_helper(remote_df = remote_df2, local_df = local_df2)
  # res2 should now have the original row (with value 13),
  # but with new version.
  # This shows that the algorithm implemented in compress_helper()
  # will result in duplication of SOME data in the database
  # under the following conditions:
  # (1) A new run of the pipeline has missing rows in local_df
  #     compared to remote_df for same metadata.
  # (2) Subsequent run of the pipeline restores the missing rows.
  # This condition is expected to be very rare, because
  # repeated local runs are expected to be conducted
  # in a sandbox or testing database.
  # The "final run" for a version will put everything into MexerDB
  # and is expected to happen infrequently
  # (maybe only once)
  # for any version of the database.
  expected2 <- local_df2 |>
    dplyr::slice(3) |>
    dplyr::mutate(
      "{PFUPipelineTools::dataset_info$valid_to_version_colname}" := version_info$current_version_int,
      "{PFUPipelineTools::dataset_info$what_to_do}" := PFUPipelineTools::dataset_info$upload_new
    )
  expect_equal(res2, expected2)
})


test_that("compress_helper() works with multiple value columns", {
  remote_df <- remote_df_func() |>
    dplyr::rename(
      value1 = value
    ) |>
    dplyr::mutate(
      value2 = value1*10
    )
  local_df <- remote_df |>
    dplyr::mutate(
      "{PFUPipelineTools::dataset_info$valid_to_version_colname}" := 2
    )

  # local_df is same as remote_df,
  # so no changes are expected.
  # Returning an no-row data frame is the right thing to do.
  res <- compress_helper(remote_df = remote_df,
                         local_df = local_df,
                         value_colname = c("value1", "value2"))
  expect_equal(nrow(res), 0)
  expect_equal(colnames(res), c(colnames(remote_df), PFUPipelineTools::dataset_info$what_to_do))
  expect_equal(colnames(res), c(colnames(local_df), PFUPipelineTools::dataset_info$what_to_do))


  # Add completely new data with a new version.
  # The metadata do not exist in remote.
  local_df2 <- local_df |>
    dplyr::mutate(
      "{IEATools::iea_cols$country}" := -9999
    )
  res2 <- compress_helper(remote_df = remote_df,
                          local_df = local_df2,
                          value_colname = c("value1", "value2"))
  res2 |>
    dplyr::filter(WhatToDo == PFUPipelineTools::dataset_info$delete_row_in_remote) |>
    nrow() |>
    expect_equal(13)
  res2 |>
    dplyr::filter(WhatToDo == PFUPipelineTools::dataset_info$upload_new) |>
    nrow() |>
    expect_equal(13)
  res2 |>
    colnames() |>
    expect_equal(c(colnames(remote_df), PFUPipelineTools::dataset_info$what_to_do))
  expect_equal(res2, local_df2 |> dplyr::mutate(
    "{PFUPipelineTools::dataset_info$what_to_do}" := PFUPipelineTools::dataset_info$upload_new))

  # Remove a row in local_df,
  # so the row needs to be removed from the remote table.
  local_df3 <- local_df[-2, ]
  res3 <- compress_helper(remote_df = remote_df,
                          local_df = local_df3,
                          value_colname = c("value1", "value2"))
  expect_equal(res3, remote_df[2, ] |>
                 dplyr::mutate(
                   "{PFUPipelineTools::dataset_info$what_to_do}" := PFUPipelineTools::dataset_info$delete_row_in_remote))

  # Adjust one of the values in local_df
  # so that changes are expected
  local_df4 <- local_df
  local_df4[2, "value1"] <- 3.1415926
  res4 <- compress_helper(remote_df = remote_df,
                          local_df = local_df4,
                          value_colname = c("value1", "value2"))
  # This should result in a change to the remote_df for 1 row
  res4 |>
    dplyr::filter(WhatToDo == PFUPipelineTools::dataset_info$replace_value_in_remote) |>
    nrow() |>
    expect_equal(1)

  # Change one value and a new version.
  # This should change the ValidToVersion column in remote and
  # upload the new version.
  local_df5 <- local_df |>
    dplyr::mutate(
      "{PFUPipelineTools::dataset_info$valid_from_version}" := 3,
      "{PFUPipelineTools::dataset_info$valid_to_version}" := 3,
    )
  local_df5[2, "value1"] <- 3.1415926
  res5 <- compress_helper(remote_df = remote_df,
                          local_df = local_df5,
                          value_colname = c("value1", "value2"))
  expect_equal(nrow(res5), 2)

})















