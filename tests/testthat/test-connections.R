test_that("get_db_conn() works as expected", {
  testthat::skip_on_ci()
  testthat::skip_on_cran()
  testthat::skip_on_covr()
  conn <- get_db_conn(user = "dbcreator")
  expect_true(DBI::dbIsValid(conn))
  DBI::dbDisconnect(conn)
})

test_that("get_mexerdb_conn() works as expected", {
  testthat::skip_on_ci()
  testthat::skip_on_cran()
  testthat::skip_on_covr()
  conn <- get_mexerdb_conn(user = "dbcreator")
  expect_true(DBI::dbIsValid(conn))
  DBI::dbDisconnect(conn)
})


test_that("get_sandboxdb_conn() works as expected", {
  testthat::skip_on_ci()
  testthat::skip_on_cran()
  testthat::skip_on_covr()
  conn <- get_sandboxdb_conn()
  expect_true(DBI::dbIsValid(conn))
  DBI::dbDisconnect(conn)
})


test_that("get_scratchmdb_conn() works as expected", {
  testthat::skip_on_ci()
  testthat::skip_on_cran()
  testthat::skip_on_covr()
  conn <- get_scratchmdb_conn()
  expect_true(DBI::dbIsValid(conn))
  DBI::dbDisconnect(conn)
})


test_that("get_scratchedb_conn() works as expected", {
  testthat::skip_on_ci()
  testthat::skip_on_cran()
  testthat::skip_on_covr()
  conn <- get_scratchedb_conn()
  expect_true(DBI::dbIsValid(conn))
  DBI::dbDisconnect(conn)
})


test_that("get_unit_testing_conn() works as expected", {
  testthat::skip_on_ci()
  testthat::skip_on_cran()
  testthat::skip_on_covr()
  conn <- get_unit_testing_conn()
  expect_true(DBI::dbIsValid(conn))
  DBI::dbDisconnect(conn)
})



