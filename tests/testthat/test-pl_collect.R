test_that("pl_collect() works as expected", {

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

  db_table_name <- "PLCollectTestTable"
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
  result1 <- pl_collect(hash1, conn = conn, decode_fks = FALSE)
  expect_equal(result1, tibble::as_tibble(test_table1))
  result2 <- pl_collect(hash2, conn = conn, decode_fks = FALSE)
  expect_equal(result2, tibble::as_tibble(test_table2))

  # Can we retain the table name column?
  result1r <- pl_collect(hash1,
                         conn = conn,
                         decode_fks = FALSE,
                         retain_table_name_col = TRUE)
  expect_true(PFUPipelineTools::hashed_table_colnames$db_table_name %in% colnames(result1r))
  expect_equal(result1r[[PFUPipelineTools::hashed_table_colnames$db_table_name]] |>
                 unique(),
               db_table_name)

  DBI::dbRemoveTable(conn, db_table_name)
})
