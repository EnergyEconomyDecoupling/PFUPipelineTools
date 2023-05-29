test_that("trim_pipeline() works as expected", {
  test_targets <- list(A = 1, B = 2, C = 3)
  test_targets |>
    trim_pipeline(last_target_to_keep = "B") |>
    expect_equal(list(A = 1, B = 2))

  test_targets_2 <- list(A = 1, A = 2, C = 3)
  test_targets_2 |>
    trim_pipeline(last_target_to_keep = "A") |>
    expect_error("length\\(last_index_to_keep\\) not equal to 1")

  test_targets_2 |>
    trim_pipeline(last_target_to_keep = "C") |>
    expect_equal(list(A = 1, A = 2, C = 3))

  test_targets_2 |>
    trim_pipeline() |>
    expect_equal(list(A = 1, A = 2, C = 3))
})
