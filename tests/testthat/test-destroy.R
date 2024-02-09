test_that("pl_destroy() works as expected", {
  conn <- DBI::dbConnect(RSQLite::SQLite(), dbname = ":memory:")
  on.exit(DBI::dbDisconnect(conn))

  expect_true(inherits(conn, "SQLiteConnection"))

  # Try with no tables
  expect_true(pl_destroy(conn, drop_tables = TRUE))

  # Add tables with foreign keys
  table1 <- tibble::tribble(~A, ~B1,
                            1, 2)
  table2 <- tibble::tribble(~B2, ~C,
                            3, 4)
  DM <- list(table1 = table1, table2 = table2) |>
    dm::as_dm() |>
    dm::dm_add_pk(table = table1, columns = A) |>
    dm::dm_add_fk(table = table2, columns = B2, ref_table = table1, ref_columns = A)
  # Add the data model
  withDM <- dm::copy_dm_to(conn, DM, set_key_constraints = TRUE, temporary = FALSE)
  # Check that the tables were uploaded
  expect_equal(length(DBI::dbListTables(conn)), 2)
  expect_equal(DBI::dbListTables(conn), c("table1", "table2"))
  # Check that we can eliminate all tables
  expect_true(pl_destroy(conn, drop_tables = TRUE))
  expect_equal(length(DBI::dbListTables(conn)), 0)
})
