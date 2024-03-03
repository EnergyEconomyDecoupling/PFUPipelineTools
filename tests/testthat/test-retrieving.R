test_that("pl_collect_from_hash() works as expected", {

  skip_on_ci()
  skip_on_cran()
  conn <- DBI::dbConnect(drv = RPostgres::Postgres(),
                         dbname = "unit_testing",
                         host = "eviz.cs.calvin.edu",
                         port = 5432,
                         user = "postgres")
  on.exit(DBI::dbDisconnect(conn))

  # Make a data frame for testing.
  # This data frame should have some columns with only 1 value
  # to test the hash returned by pl_upsert()

  db_table_name <- "PLCollectFromHashTestTable"
  if (db_table_name %in% DBI::dbListTables(conn)) {
    DBI::dbRemoveTable(conn, db_table_name)
  }
  test_table1 <- data.frame(key1 = as.integer(c(1, 1, 1)),
                            key2 = as.integer(c(1, 2, 3)),
                            val = c("A", "B", "C"))
  test_table1_empty <- test_table1[0, ]
  DM <- list(test_table1_empty) |>
    magrittr::set_names(db_table_name) |>
    dm::as_dm() |>
    dm::dm_add_pk({{db_table_name}}, c(key1, key2))

  dm::copy_dm_to(dest = conn, dm = DM, temporary = FALSE)
  hash1 <- test_table1 |>
    pl_upsert(db_table_name = db_table_name,
              conn = conn,
              in_place = TRUE)

  # Add some additional data
  test_table2 <- data.frame(key1 = as.integer(c(2, 2, 2)),
                            key2 = as.integer(c(1, 2, 3)),
                            val = c("D", "E", "F"))
  hash2 <- test_table2 |>
    pl_upsert(db_table_name = db_table_name,
              conn = conn,
              in_place = TRUE)

  expect_equal(colnames(hash1),
               c(PFUPipelineTools::hashed_table_colnames$db_table_name,
                 "key1",
                 PFUPipelineTools::hashed_table_colnames$nested_hash_col_name))
  expected_hash <- "e4c5198aa04c376d9096111092794069"
  hash1 |>
    dplyr::select(dplyr::all_of(PFUPipelineTools::hashed_table_colnames$nested_hash_col_name)) |>
    unlist() |>
    unname() |>
    expect_equal(expected_hash)

  # Try to upsert using a column name in the table
  test_table_1_tbl_name_col <- test_table1 |>
    dplyr::mutate(
      "{PFUPipelineTools::hashed_table_colnames$db_table_name}" := db_table_name
    )
  test_table_1_tbl_name_col |>
    pl_upsert(conn = conn, in_place = TRUE) |>
    dplyr::select(dplyr::all_of(PFUPipelineTools::hashed_table_colnames$nested_hash_col_name)) |>
    unlist() |>
    unname() |>
    expect_equal(expected_hash)

  # Do we round-trip successfully?
  result1 <- pl_collect_from_hash(hash1, conn = conn, decode_fks = FALSE)
  expect_equal(result1, tibble::as_tibble(test_table1))
  result2 <- pl_collect_from_hash(hash2, conn = conn, decode_fks = FALSE)
  expect_equal(result2, tibble::as_tibble(test_table2))

  # Can we retain the table name column?
  result1r <- pl_collect_from_hash(hash1,
                                   conn = conn,
                                   decode_fks = FALSE,
                                   retain_table_name_col = TRUE)
  expect_true(PFUPipelineTools::hashed_table_colnames$db_table_name %in% colnames(result1r))
  expect_equal(result1r[[PFUPipelineTools::hashed_table_colnames$db_table_name]] |>
                 unique(),
               db_table_name)

  DBI::dbRemoveTable(conn, db_table_name)
})


test_that("passing filters in ... works as expected", {
  # An example function that passes filter queries in ...
  my_filter <- function(.df, ...) {

    args <- rlang::enquos(...)

    .df |>
      dplyr::filter(!!!args)
  }

  data <- data.frame(
    x = c(1, 2, 3, 4, 5),
    y = c(10, 20, 30, 40, 50)
  )

  # Try with a single criterion in ...
  expect_equal(my_filter(data, x > 2),
               data.frame(x = c(3, 4, 5), y = c(30, 40, 50)))
  expect_equal(my_filter(data, x == 5),
               data.frame(x = 5, y = 50))
  # Try with two criteria in ...
  expect_equal(my_filter(data, x > 2, y < 40),
               data.frame(x = 3, y = 30))
})


test_that("pl_nat_filter() works as expected", {
  skip_on_ci()
  skip_on_cran()
  conn <- DBI::dbConnect(drv = RPostgres::Postgres(),
                         dbname = "unit_testing",
                         host = "eviz.cs.calvin.edu",
                         port = 5432,
                         user = "postgres")
  on.exit(DBI::dbDisconnect(conn))

  db_table_name <- "PLFilterCollectTestTable"
  db_country_name <- "PLFilterCollectTestCountry"
  if (db_table_name %in% DBI::dbListTables(conn)) {
    DBI::dbRemoveTable(conn, db_table_name)
  }
  if (db_country_name %in% DBI::dbListTables(conn)) {
    DBI::dbRemoveTable(conn, db_country_name)
  }

  # Create a table to upload
  PLFilterCollectTestTable <- data.frame(MyCountry = as.integer(c(1, 2, 3, 1)),
                                         MyValue = c(3.1415, 2.71828, 42, 5.67e-8))
  PLFilterCollectTestCountry <- data.frame(CountryID = as.integer(c(1, 2, 3)),
                                           Country = c("USA", "ZAF", "GHA"))
  DM <- dm::as_dm(list(PLFilterCollectTestTable = PLFilterCollectTestTable,
                       PLFilterCollectTestCountry = PLFilterCollectTestCountry)) |>
    dm::dm_add_fk(table = PLFilterCollectTestTable,
                  columns = MyCountry,
                  ref_table = PLFilterCollectTestCountry,
                  ref_columns = CountryID)
  dm::copy_dm_to(conn, DM, set_key_constraints = TRUE, temporary = FALSE)

  expect_equal(DBI::dbReadTable(conn, "PLFilterCollectTestTable"), PLFilterCollectTestTable)
  expect_equal(DBI::dbReadTable(conn, "PLFilterCollectTestCountry"), PLFilterCollectTestCountry)

  pl_nat_filter("PLFilterCollectTestTable",
                MyCountry == "USA",
                conn = conn,
                collect = TRUE) |>
    expect_equal(tibble::tribble(~MyCountry, ~MyValue,
                                 "USA", 3.1415,
                                 "USA", 5.67e-8))

  # Now try with the schema and fk tables pre-supplied
  fk_tables <- get_all_fk_tables(conn = conn, schema = DM)
  pl_nat_filter("PLFilterCollectTestTable",
                MyCountry == "USA",
                conn = conn,
                collect = TRUE,
                schema = DM,
                fk_parent_tables = fk_tables) |>
    expect_equal(tibble::tribble(~MyCountry, ~MyValue,
                                 "USA", 3.1415,
                                 "USA", 5.67e-8))

  pl_nat_filter("PLFilterCollectTestTable",
                MyCountry %in% c("USA", "ZAF"),
                conn = conn,
                collect = TRUE) |>
    expect_equal(tibble::tribble(~MyCountry, ~MyValue,
                                 "USA", 3.1415,
                                 "ZAF", 2.71828,
                                 "USA", 5.67e-8))

  # Try without collecting
  uncollected <- pl_nat_filter(db_table_name = "PLFilterCollectTestTable",
                               MyCountry == "USA",
                               conn = conn,
                               schema = DM,
                               fk_parent_tables = fk_tables)
  expect_true(dplyr::is.tbl(uncollected))
  uncollected |>
    dplyr::collect() |>
    expect_equal(tibble::tribble(~MyCountry, ~MyValue,
                                 "USA", 3.1415,
                                 "USA", 5.67e-8))

  # Try without a filter expression
  pl_nat_filter("PLFilterCollectTestTable",
                conn = conn,
                collect = TRUE) |>
    expect_equal(tibble::tribble(~MyCountry, ~MyValue,
                                 "USA", 3.1415,
                                 "ZAF", 2.71828,
                                 "GHA", 42,
                                 "USA", 5.67e-8))

  DBI::dbRemoveTable(conn = conn, name = db_table_name)
  DBI::dbRemoveTable(conn = conn, name = db_country_name)
})
