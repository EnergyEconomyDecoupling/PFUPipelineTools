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
    PFUPipelineTools::pl_upsert(db_table_name, conn, in_place = TRUE)

  # Add some additional data
  test_table2 <- data.frame(key1 = as.integer(c(2, 2, 2)),
                            key2 = as.integer(c(1, 2, 3)),
                            val = c("D", "E", "F"))
  hash2 <- test_table2 |>
    PFUPipelineTools::pl_upsert(db_table_name, conn, in_place = TRUE)

  expect_equal(colnames(hash1),
               c(PFUPipelineTools::hashed_table_colnames$db_table_name,
                 "key1",
                 PFUPipelineTools::hashed_table_colnames$nested_hash_col_name))
  hash1 |>
    # dplyr::select(PFUPipelineTools::hashed_table_colnames$nested_hash_col_name) |>
    dplyr::select(dplyr::all_of(PFUPipelineTools::hashed_table_colnames$nested_hash_col_name)) |>
    unlist() |>
    unname() |>
    expect_equal("e4c5198aa04c376d9096111092794069")

  # Do we round-trip successfully?
  result1 <- pl_collect(hash1, conn, decode_fks = FALSE)
  expect_equal(result1, tibble::as_tibble(test_table1))
  result2 <- pl_collect(hash2, conn, decode_fks = FALSE)
  expect_equal(result2, tibble::as_tibble(test_table2))

  DBI::dbRemoveTable(conn, db_table_name)
})
