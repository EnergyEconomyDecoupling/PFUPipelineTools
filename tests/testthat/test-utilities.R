test_that("self_name() works as intended", {
  c("a", "b", "c") |>
    self_name() |>
    expect_equal(c(a = "a", b = "b", c = "c"))

  list("A", "B", "C") |>
    self_name() |>
    expect_equal(list(A = "A", B = "B", C = "C"))
})


test_that("inboard_filter_copy() works as expected", {
  conn <- DBI::dbConnect(RSQLite::SQLite(), ":memory:")
  on.exit(DBI::dbDisconnect(conn))

  # Make a table for testing
  source_table <- data.frame(Country = as.integer(c(1, 1, 1, 2, 2, 2, 3, 3, 3)),
                             Year = as.integer(c(1970, 1971, 1972, 1970, 1971, 1972, 1970, 1971, 1972)),
                             X = c(42:50))

  DM <- dm::as_dm(list(source = source_table[0, ],
                       dest = source_table[0, ])) |>
    dm::dm_add_pk(source, columns = c(Country, Year)) |>
    dm::dm_add_pk(dest, columns = c(Country, Year))

  dm::copy_dm_to(dest = conn, dm = DM, set_key_constraints = TRUE)

  DBI::dbWriteTable(conn, "source", source_table, overwrite = TRUE)
  source_tbl <- dplyr::tbl(conn, "source")

  DBI::dbWriteTable(conn, "dest", source_table, overwrite = TRUE)
  dest_tbl <- dplyr::tbl(conn, "dest")

  inboard_filter_copy(source = "source",
                      dest = "dest",
                      countries = c(1, 2),
                      years = c(1970, 1972),
                      empty_dest = TRUE,
                      in_place = TRUE,
                      conn = conn,
                      schema = DM,
                      fk_parent_tables = NULL)


  # Get the new table
  res <- dplyr::tbl(conn, "dest") |>
    dplyr::collect()
  expected <- tibble::tribble(~Country, ~Year, ~X,
                              1, 1970, 42,
                              1, 1972, 44,
                              2, 1970, 45,
                              2, 1972, 47)
  expect_equal(res, expected)
})
