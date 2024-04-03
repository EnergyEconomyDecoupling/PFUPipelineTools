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


test_that("encode_matsindf() works as expected", {
  ptype <- "Products"
  itype <- "Industries"
  tidy <- data.frame(Country  = c( "GH",  "GH",  "GH",  "GH",  "GH",  "GH",  "GH",  "US",  "US",  "US",  "US"),
                     Year     = c( 1971,  1971,  1971,  1971,  1971,  1971,  1971,  1980,  1980,  1980,  1980),
                     matname = c(   "U",   "U",   "Y",   "Y",   "Y",   "V",   "V",   "U",   "U",   "Y",   "Y"),
                     row      = c(  "p1",  "p2",  "p1",  "p2",  "p2",  "i1",  "i2",  "p1",  "p1",  "p1",  "p2"),
                     col      = c(  "i1",  "i2",  "i1",  "i2",  "i3",  "p1",  "p2",  "i1",  "i2",  "i1",  "i2"),
                     rowtypes = c(ptype, ptype, ptype, ptype, ptype, itype, itype, ptype, ptype, ptype, ptype),
                     coltypes = c(itype, itype, itype, itype, itype, ptype, ptype, itype, itype, itype, itype),
                     matval  = c(   11  ,  12,    13 ,   14 ,   15 ,   16 ,   17 ,   49 ,   50 ,   51 ,   52),
                     stringsAsFactors = FALSE) |>
    dplyr::group_by(Country, Year, matname)
  mats <- tidy |>
    matsindf::collapse_to_matrices(matname = "matrix", rownames = "row", colnames = "col",
                                   rowtypes = "rowtypes", coltypes = "coltypes",
                                   matval = "matval", matrix_class = "Matrix") |>
    dplyr::ungroup()

  industry_index_map <- data.frame(index = as.integer(c(1, 2, 3)),
                                   name = c("i1", "i2", "i3"))
  product_index_map <- data.frame(index = as.integer(c(1, 2)),
                                  name = c("p1", "p2"))
  index_map <- list(Industries = industry_index_map,
                    Products = product_index_map)
  res <- mats |>
    encode_matsindf(index_map = index_map)
  expect_equal(res |>
                 dplyr::filter(matname == "U", Year == 1971) |>
                 nrow(),
               2)
  expect_equal(res |>
                 dplyr::filter(matname == "V", Year == 1971) |>
                 nrow(),
               2)
  expect_equal(res |>
                 dplyr::filter(matname == "Y", Year == 1971) |>
                 nrow(),
               3)
  expect_equal(res |>
                 dplyr::filter(matname == "U", Year == 1980) |>
                 nrow(),
               2)
  expect_equal(res |>
                 dplyr::filter(matname == "Y", Year == 1980) |>
                 nrow(),
               2)

  # Check a few results
  expect_equal(res |>
                 dplyr::filter(matname == "U", Year == 1971, i == 1, j == 1) |>
                 magrittr::extract2("x"),
               11)
  expect_equal(res |>
                 dplyr::filter(matname == "Y", Year == 1971, i == 2, j == 3) |>
                 magrittr::extract2("x"),
               15)
  expect_equal(res |>
                 dplyr::filter(matname == "U", Year == 1980, i == 1, j == 2) |>
                 magrittr::extract2("x"),
               50)
  expect_equal(res |>
                 dplyr::filter(matname == "Y", Year == 1980, i == 2, j == 2) |>
                 magrittr::extract2("x"),
               52)
})


test_that("decode_matsindf() works as expected", {
  # Create an example data frame.
  ptype <- "Products"
  itype <- "Industries"
  tidy <- data.frame(Country  = c( "GH",  "GH",  "GH",  "GH",  "GH",  "GH",  "GH",  "US",  "US",  "US",  "US"),
                     Year     = c( 1971,  1971,  1971,  1971,  1971,  1971,  1971,  1980,  1980,  1980,  1980),
                     matname = c(   "U",   "U",   "Y",   "Y",   "Y",   "V",   "V",   "U",   "U",   "Y",   "Y"),
                     row      = c(  "p1",  "p2",  "p1",  "p2",  "p2",  "i1",  "i2",  "p1",  "p1",  "p1",  "p2"),
                     col      = c(  "i1",  "i2",  "i1",  "i2",  "i3",  "p1",  "p2",  "i1",  "i2",  "i1",  "i2"),
                     rowtypes = c(ptype, ptype, ptype, ptype, ptype, itype, itype, ptype, ptype, ptype, ptype),
                     coltypes = c(itype, itype, itype, itype, itype, ptype, ptype, itype, itype, itype, itype),
                     matval  = c(   11  ,  12,    13 ,   14 ,   15 ,   16 ,   17 ,   49 ,   50 ,   51 ,   52),
                     stringsAsFactors = FALSE) |>
    dplyr::group_by(Country, Year, matname)
  mats <- tidy |>
    matsindf::collapse_to_matrices(matname = "matrix", rownames = "row", colnames = "col",
                                   rowtypes = "rowtypes", coltypes = "coltypes",
                                   matval = "matval", matrix_class = "Matrix") |>
    dplyr::ungroup()

  industry_index_map <- data.frame(index = as.integer(c(1, 2, 3)),
                                   name = c("i1", "i2", "i3"))
  product_index_map <- data.frame(index = as.integer(c(1, 2)),
                                  name = c("p1", "p2"))
  index_map <- list(Industries = industry_index_map,
                    Products = product_index_map)

  rctypes <- tibble::tribble(~matname, ~rowtype, ~coltype,
                             "U",       ptype,    itype,
                             "V",       itype,    ptype,
                             "Y",       ptype,    itype)
  encoded <- mats |>
    encode_matsindf(index_map = index_map)

  # Decode the encoded data frame
  res <- encoded |>
    decode_matsindf(index_map = index_map,
                    rctypes = rctypes,
                    matrix_class = "Matrix",
                    wide_by_matrices = FALSE)

  # Check that things are the same
  dplyr::full_join(mats, res, by = c("Country", "Year", "matname")) |>
    dplyr::mutate(
      OK = matsbyname:::equal_byname(matval.x, matval.y)
    ) |>
    magrittr::extract2("OK") |>
    unlist() |>
    all() |>
    expect_true()
})
