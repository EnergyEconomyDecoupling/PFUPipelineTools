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
                         host = "mexer.site",
                         port = 5432,
                         user = "mkh2")
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
                 magrittr::extract2("value"),
               11)
  expect_equal(res |>
                 dplyr::filter(matname == "Y", Year == 1971, i == 2, j == 3) |>
                 magrittr::extract2("value"),
               15)
  expect_equal(res |>
                 dplyr::filter(matname == "U", Year == 1980, i == 1, j == 2) |>
                 magrittr::extract2("value"),
               50)
  expect_equal(res |>
                 dplyr::filter(matname == "Y", Year == 1980, i == 2, j == 2) |>
                 magrittr::extract2("value"),
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


test_that("encode_matsindf() works with a NULL matrix", {
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

  # Now set one of the matrices to NULL,
  # the U matrix in 1971.
  mats$matval[1] <- list(NULL)

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
  # Encode the data frame with a NULL position.
  encoded <- mats |>
    encode_matsindf(index_map = index_map)
  expect_equal(encoded |>
                 dplyr::filter(Year == 1971, matname == "U") |>
                 nrow(), 0)
})


test_that("round_double_cols() works as expected", {
  # Try some rounding first, to understand how it works.
  expect_true(signif(2, digits = 2) == 2)
  expect_true(signif(2 + 1e-10, digits = 2) == 2)
  expect_true(signif(1.5, digits = 2) == 1.5)
  expect_false(signif(1.3, digits = 2) == 1.5)
  expect_true(signif(1.5, digits = 1) == 2)

  # These are not equal,
  # because the trailing "1" is significant at 10 digits.
  # So the test for equality fails.
  expect_false(signif(1.500001, digits = 10) == 1.5)
  expect_false(signif(1.5000001, digits = 10) == 1.5)
  expect_false(signif(1.50000001, digits = 10) == 1.5)
  expect_false(signif(1.500000001, digits = 10) == 1.5)
  # Now the trailing "1" is in the 11th digit, so is insignificant.
  # So the test for equality is now true.
  expect_true( signif(1.5000000001, digits = 10) == 1.5)

  # Let's find the sensitivity of the "==" operator.
  expect_false(signif(1.5000000001, digits = 11) == 1.5)
  expect_false(signif(1.50000000001, digits = 12) == 1.5)
  expect_false(signif(1.500000000001, digits = 13) == 1.5)
  expect_false(signif(1.5000000000001, digits = 14) == 1.5)
  expect_false(signif(1.50000000000001, digits = 16) == 1.5)
  expect_false(signif(1.500000000000001, digits = 17) == 1.5)
  # Looks like the sensitivity for the "==" operator is 17 digits.
  # When the trailing 1 is in the 17th digit
  # and we compare at 18 digits of significance,
  # the equality test is successful.
  expect_true( signif(1.5000000000000001, digits = 18) == 1.5)
  expect_true( signif(1.50000000000000001, digits = 19) == 1.5)
  expect_true( signif(1.500000000000000001, digits = 20) == 1.5)
  expect_true( signif(1.5000000000000000001, digits = 20) == 1.5)
  expect_true( signif(1.50000000000000000001, digits = 21) == 1.5)
  expect_true( signif(1.500000000000000000001, digits = 22) == 1.5)

  # Now test some specific rounding
  # We were getting these numbers in the pipeline.
  expect_true(signif(42972.72604058508295565844, digits = 15) == 42972.7260405851)
  expect_true(signif(42972.72604058508295565844, digits = 16) == 42972.72604058508)

  expect_true(signif(42972.72604058507567970082, digits = 15) == 42972.7260405851)
  expect_true(signif(42972.72604058507567970082, digits = 16) == 42972.72604058508)
  expect_true(signif(42972.72604058507567970082, digits = 17) == 42972.726040585076)

  # What happens with NA values?
  expect_true(signif(NA, digits = 2) |> is.na())


  # Now try the function with data frames.


  data.frame(pi = pi) |>
    round_double_cols(digits = 3) |>
    magrittr::extract2("pi") |>
    expect_equal(3.14)

  res <- data.frame(name = "pi", pi = pi) |>
    round_double_cols(digits = 3)
  res |>
    magrittr::extract2("pi") |>
    expect_equal(3.14)
  res |>
    magrittr::extract2("name") |>
    expect_equal("pi")


  res2 <- data.frame(name = c("pi", "pi2"),
                     # Have to make these integers ("L")
                     # so that the function leaves this column alone.
                     million = c(1000000L, 1000001L),
                     pi = c(pi, pi+1)) |>
    round_double_cols(digits = 3)
  res2 |>
    magrittr::extract2("pi") |>
    magrittr::extract2(1) |>
    expect_equal(3.14)
  res2 |>
    magrittr::extract2("pi") |>
    magrittr::extract2(2) |>
    expect_equal(4.14)
  res2 |>
    magrittr::extract2("million") |>
    magrittr::extract2(1) |>
    expect_equal(1000000)
  res2 |>
    magrittr::extract2("million") |>
    magrittr::extract2(2) |>
    expect_equal(1000001)

  # Test a data frame without any double-precision columns
  df2 <- tibble::tribble(~first, ~last,
                         "John", "Lennon",
                         "Paul", "McCartney",
                         "George", "Harrison",
                         "Ringo", "Starr")
  df2 |>
    round_double_cols() |>
    expect_equal(df2)

  # What happens when an integer is stored as a double-precision?
  # This could happen when we store metadata columns.
  res3 <- data.frame(name = c("pi", "pi2"),
                     # Have to make these integers ("L")
                     # so that the function leaves this column alone.
                     million = c(1000000, 1000001),
                     pi = c(pi, pi+1)) |>
    round_double_cols(digits = 15)
  res3 |>
    magrittr::extract2("million") |>
    magrittr::extract2(1) |>
    expect_equal(1000000)
  res3 |>
    magrittr::extract2("million") |>
    magrittr::extract2(2) |>
    expect_equal(1000001)
})


























