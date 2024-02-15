test_that("release_target() works as expected", {

  df <- data.frame(names = c("A", "B"), values = c(1, 2))
  # Create a temp folder and pinboard
  tdir <- tempdir()
  pinboard <- pins::board_folder(tdir)

  # Check not releeasing
  no_release <- release_target(tdir, targ = df, pin_name = "df", release = FALSE)
  expect_equal(no_release, "Release not requested.")

  # Check doing a release
  yes_release <- release_target(tdir, targ = df, pin_name = "df", release = TRUE) |>
    suppressMessages()
  expect_equal(yes_release, "df")
  expect_true("df" %in% list.files(path = tdir))
  recursed <- list.files(path = tdir, recursive = TRUE)
  expect_true(startsWith(recursed[[1]], "df"))
  expect_true(endsWith(recursed[[1]], "data.txt"))
  expect_true(startsWith(recursed[[2]], "df"))
  expect_true(endsWith(recursed[[2]], "df.rds"))
  # Delete the temporary folder
  unlink(tdir, recursive = TRUE, force = TRUE)
})


test_that("pl_calc_hash() works as expected", {
  # Example data frame
  DF <- tibble::tribble(~Country, ~Year, ~Value,
                        "USA", 1967, 42,
                        "ZAF", 1967, 43)
  the_hash <- DF |>
    pl_calc_hash(table_name = "MyTable")

  expect_equal(names(the_hash), c(PFUPipelineTools::hashed_table_colnames$db_table_name,
                                  "Year",
                                  PFUPipelineTools::hashed_table_colnames$nested_col_name))
  expect_equal(nrow(the_hash), 1)
})
