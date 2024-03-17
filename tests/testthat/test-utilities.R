test_that("self_name() works as intended", {
  c("a", "b", "c") |>
    self_name() |>
    expect_equal(c(a = "a", b = "b", c = "c"))

  list("A", "B", "C") |>
    self_name() |>
    expect_equal(list(A = "A", B = "B", C = "C"))
})


test_that("inboard_filter_copy() works as expected", {
  skip_on_ci()
  skip_on_cran()
  conn <- DBI::dbConnect(drv = RPostgres::Postgres(),
                         dbname = "unit_testing",
                         host = "eviz.cs.calvin.edu",
                         port = 5432,
                         user = "postgres")
  on.exit(DBI::dbDisconnect(conn))

  # Make a table for testing
  source_table <- data.frame(Country = as.integer(c(1, 1, 1, 2, 2, 2, 3, 3, 3)),
                             Year = as.integer(c(1970, 1971, 1972, 1970, 1971, 1972, 1970, 1971, 1972)),
                             X = c(42:50))
  country_table <- data.frame(CountryID = as.integer(c(1, 2, 3)),
                              Country = c("USA", "ZAF", "GHA"))
  year_table <- data.frame(YearID = as.integer(c(1970, 1971, 1972)),
                           Year = as.integer(c(1970, 1971, 1972)))

  DM <- dm::as_dm(list(source = source_table[0, ],
                       dest = source_table[0, ],
                       Country = country_table,
                       Year = year_table)) |>
    dm::dm_add_pk(source, columns = c(Country, Year)) |>
    dm::dm_add_pk(dest, columns = c(Country, Year)) |>
    dm::dm_add_fk(table = source, columns = Country, ref_table = Country, ref_columns = CountryID) |>
    dm::dm_add_fk(table = source, columns = Year, ref_table = Year, ref_columns = YearID) |>
    dm::dm_add_fk(table = dest, columns = Country, ref_table = Country, ref_columns = CountryID) |>
    dm::dm_add_fk(table = dest, columns = Year, ref_table = Year, ref_columns = YearID)

  dm::copy_dm_to(dest = conn, dm = DM, set_key_constraints = TRUE, temporary = FALSE)
  dplyr::rows_insert(x = dplyr::tbl(conn, "source"),
                     y = source_table,
                     by = c("Country", "Year"),
                     conflict = "ignore",
                     copy = TRUE,
                     in_place = TRUE)

  fk_parent_tables <- get_all_fk_tables(conn,
                                        schema = DM,
                                        collect = TRUE)
  the_hash <- inboard_filter_copy(source = "source",
                                  dest = "dest",
                                  countries = c("USA", "ZAF"),
                                  years = c(1970, 1972),
                                  empty_dest = TRUE,
                                  in_place = TRUE,
                                  conn = conn,
                                  schema = DM,
                                  fk_parent_tables = fk_parent_tables)
  expect_equal(nrow(the_hash), 4)
  expect_equal(colnames(the_hash),
               c(PFUPipelineTools::hashed_table_colnames$db_table_name,
                 IEATools::iea_cols$country,
                 IEATools::iea_cols$year,
                 PFUPipelineTools::hashed_table_colnames$nested_hash_colname))
  expect_equal(unique(the_hash[[PFUPipelineTools::hashed_table_colnames$db_table_name]]),
               "dest")


  # Get the new table
  res <- dplyr::tbl(conn, "dest") |>
    dplyr::collect()
  expected <- tibble::tribble(~Country, ~Year, ~X,
                              1, 1970, 42,
                              1, 1972, 44,
                              2, 1970, 45,
                              2, 1972, 47)
  expect_equal(res, expected)

  DBI::dbRemoveTable(conn, "source")
  DBI::dbRemoveTable(conn, "dest")
  DBI::dbRemoveTable(conn, "Country")
  DBI::dbRemoveTable(conn, "Year")
})


test_that("encode_fk_values() works as expected", {
  fk_parent_tables <- list(Country = tibble::tribble(~CountryID, ~Country,
                                                     1, "USA",
                                                     2, "ZAF",
                                                     3, "GHA"))
  res <- encode_fk_values(c("USA", "USA", "GHA"),
                          fk_table_name = "Country",
                          fk_parent_tables = fk_parent_tables)
  expect_equal(res, c(1, 1, 3))
  # Try with only one country
  expect_equal(encode_fk_values("ZAF",
                                fk_table_name = "Country",
                                fk_parent_tables = fk_parent_tables),
               2)
  # Try with an unknown country
  expect_error(encode_fk_values("CAN",
                                fk_table_name = "Country",
                                fk_parent_tables = fk_parent_tables),
               regexp = "Unknown fk values in encode_fk_values")
  # Try with unknown countries
  expect_error(encode_fk_values(c("GBR", "CAN"),
                                fk_table_name = "Country",
                                fk_parent_tables = fk_parent_tables),
               regexp = "Unknown fk values in encode_fk_values")
})


test_that("decode_fk_keys() works as expected", {
  fk_parent_tables <- list(Country = data.frame(CountryID = as.integer(c(1, 2, 3)),
                                                Country = c("USA", "ZAF", "GHA")))
  res <- decode_fk_keys(c(1, 1, 3),
                        fk_table_name = "Country",
                        fk_parent_tables = fk_parent_tables)
  expect_equal(res, c("USA", "USA", "GHA"))
  # Try with only one country
  expect_equal(decode_fk_keys(2,
                              fk_table_name = "Country",
                              fk_parent_tables = fk_parent_tables),
               "ZAF")
  # Try with an unknown country
  expect_error(decode_fk_keys(5,
                              fk_table_name = "Country",
                              fk_parent_tables = fk_parent_tables),
               regexp = "Unknown fk keys in decode_fk_values")
  # Try with unknown countries
  expect_error(decode_fk_keys(c(42, 43),
                              fk_table_name = "Country",
                              fk_parent_tables = fk_parent_tables),
               regexp = "Unknown fk keys in decode_fk_values")
})
