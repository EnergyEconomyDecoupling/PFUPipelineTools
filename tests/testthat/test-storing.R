test_that("release_target() works as expected", {

  df <- data.frame(names = c("A", "B"), values = c(1, 2))
  # Create a temp folder and pinboard
  tdir <- tempdir()
  pinboard <- pins::board_folder(tdir)

  # Check not releeasing
  no_release <- release_target(tdir, targ = df, pin_name = "df", release = FALSE)
  expect_equal(no_release, "Release not requested.")
  expect_equal(list.files(path = tdir), character())

  # Check doing a release
  yes_release <- release_target(tdir, targ = df, pin_name = "df", release = TRUE)
  expect_equal(yes_release, "df")
  expect_equal(list.files(path = tdir), "df")
  recursed <- list.files(path = tdir, recursive = TRUE)
  expect_true(startsWith(recursed[[1]], "df"))
  expect_true(endsWith(recursed[[1]], "data.txt"))
  expect_true(startsWith(recursed[[2]], "df"))
  expect_true(endsWith(recursed[[2]], "df.rds"))
  # Delete the temporary folder
  unlink(tdir, recursive = TRUE, force = TRUE)
})


test_that("stash_cache() works as expected", {

})
