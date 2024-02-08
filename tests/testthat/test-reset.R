test_that("pl_destroy() works as expected", {
  conn <- DBI::dbConnect(RSQLite::SQLite(), ":memory:")
  on.exit(DBI::dbDisconnect(conn))

  df <- tibble::tribble(~A, ~B,
                        1,  2,
                        3,  4)
  DBI::dbWriteTable(conn, name = "df", df)
  table_names <- DBI::dbListTables(conn)
  expect_equal(table_names, "df")

  destroyed <- pl_destroy(conn)
  expect_true(destroyed)
  table_names_2 <- DBI::dbListTables(conn)
  expect_equal(length(table_names_2), 0)
})
