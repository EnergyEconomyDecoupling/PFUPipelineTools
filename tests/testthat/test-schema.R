test_that("load_schema_table() works as expected", {
  # There are too many path dependencies to work on CI.
  skip_on_ci()
  st <- load_schema_table(version = "v1.4")
  expect_true("Table" %in% colnames(st))
  expect_true("colname" %in% colnames(st))
  expect_true("coldatatype" %in% colnames(st))
  expect_true("fk.table" %in% colnames(st))
  expect_true("fk.colname" %in% colnames(st))
})


test_that("schema_dm() works as expected", {
  # There are too many path dependencies to work on CI.
  skip_on_ci()
  clpfu_dm <- load_schema_table(version = "v1.4") |>
    schema_dm()
  clpfu_dm |>
    dm::dm_get_all_fks() |>
    nrow() |>
    expect_gt(10)
  clpfu_dm |>
    dm::dm_get_all_pks() |>
    nrow() |>
    expect_gt(10)
})


test_that("schema_dm() fails with unknown data type", {
  st <- load_schema_table(version = "v1.4")
  st[[1, "coldatatype"]] <- "bogus"
  st |>
    schema_dm() |>
    expect_error(regexp = "Unknown data type")
})
