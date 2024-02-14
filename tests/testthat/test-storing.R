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


# test_that("stash_cache() works as expected", {
#   # Try without a release
#   expect_equal(stash_cache(release = FALSE), "Release not requested.")
#
#   # Now try with temporary folders and a bogus target
#   tdir <- tempdir()
#   pipeline_caches_folder <- file.path(tdir, "pipeline_caches_folder")
#   dir.create(pipeline_caches_folder, recursive = TRUE)
#   cache_folder <- file.path(tdir, "cache_folder")
#   dir.create(cache_folder, recursive = TRUE)
#   df <- data.frame(names = c("A", "B"), values = c(1, 2))
#   saveRDS(df, file = file.path(cache_folder, "df.rds"))
#   res <- stash_cache(pipeline_caches_folder = pipeline_caches_folder,
#                      cache_folder = cache_folder,
#                      file_prefix = "prefix",
#                      release = TRUE)
#   expect_equal(res, "prefix")
#   # Check that a file now exists in the pipeline_caches_folder
#   yeardir <- file.path(pipeline_caches_folder, list.files(pipeline_caches_folder))
#   monthdir <- file.path(yeardir, list.files(yeardir))
#   fname <- list.files(monthdir)
#   expect_true(startsWith(fname, "prefix"))
#   expect_true(endsWith(fname, ".zip"))
#
#   # Delete all temporary files
#   unlink(tdir, recursive = TRUE, force = TRUE)
#
#   # The next bit fails on only one of the GitHub actions platforms.
#   # So don't worry about running on CI.
#   skip_on_ci()
#
#   # Try again after deleting the temporary files.
#   stash_cache(pipeline_caches_folder = pipeline_caches_folder,
#               cache_folder = cache_folder,
#               file_prefix = "prefix",
#               release = TRUE) |>
#     expect_error(regexp = "cannot open the connection") |>
#     expect_error(regexp = "copying of pipeline cache unsuccessful")
# })

