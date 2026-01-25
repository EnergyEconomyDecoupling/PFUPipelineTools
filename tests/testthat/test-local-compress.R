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

  compress_helper(remote_df = remote_df, local_df = local_df)

  # There should be no rows in the change_remote list



  # Try with one new value (change to negative value)
  local_df_one_new_value <- local_df
  local_df_one_new_value[3, "value"] <- -13
  compress_helper(remote_df = remote_df,
                  local_df = local_df_one_new_value)
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
