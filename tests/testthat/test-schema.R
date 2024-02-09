test_that("load_schema_table() works as expected", {
  # There are too many path dependencies to work on CI.
  skip_on_ci()
  st <- load_schema_table(version = "v1.4")
  expect_true("Table" %in% colnames(st))
  expect_true("colname" %in% colnames(st))
  expect_true("coldatatype" %in% colnames(st))
  expect_true("fk.table" %in% colnames(st))
  expect_true("fk.colname" %in% colnames(st))
})


test_that("schema_dm() works as expected", {
  # There are too many path dependencies to work on CI.
  skip_on_ci()
  clpfu_dm <- load_schema_table(version = "v1.4") |>
    schema_dm()
  clpfu_dm |>
    dm::dm_get_all_fks() |>
    nrow() |>
    expect_gt(10)
  clpfu_dm |>
    dm::dm_get_all_pks() |>
    nrow() |>
    expect_gt(10)
})


test_that("schema_dm() fails with unknown data type", {
  st <- tibble::tribble(~Table, ~colname, ~coldatatype, ~fk.table, ~fk.colname,
                        "Country", "Country_ID", "bogus", "NA", "NA",
                        "Country", "Country", "text", "NA", "NA",
                        "Country", "Description", "text", "NA", "NA")
  st |>
    schema_dm() |>
    expect_error(regexp = "Unknown data type: 'bogus' in schema_dm")
})


test_that("load_simple_tables() works as expected", {
  skip_on_ci()
  simple_tables <- load_simple_tables(version = "v1.4")
  expect_true("Year" %in% names(simple_tables))
  expect_true("Method" %in% names(simple_tables))
})


test_that("upload_schema_and_simple_tables() works as expected", {
  skip_on_ci()
  conn <- DBI::dbConnect(drv = RPostgres::Postgres(),
                         dbname = "playground",
                         host = "eviz.cs.calvin.edu",
                         port = 5432,
                         user = "mkh2")
  on.exit(DBI::dbDisconnect(conn))
  # Add tables with foreign keys
  t1 <- matrix(nrow = 0, ncol = 2, dimnames = list(c(), c("Name_ID", "Name"))) |>
    as.data.frame() |>
    dplyr::mutate(
      Name_ID = as.integer(Name_ID),
      Name = as.character(Name)
    )
  t2 <- matrix(nrow = 0, ncol = 2, dimnames = list(c(), c("Name_ID", "Occupation"))) |>
    as.data.frame() |>
    dplyr::mutate(
      Name_ID = as.integer(Name_ID),
      Occupation = as.character(Occupation)
    )

  DM <- list(table1 = t1, table2 = t2) |>
    dm::as_dm() |>
    dm::dm_add_pk(table = table1, columns = Name_ID, autoincrement = TRUE) |>
    dm::dm_add_pk(table = table2, columns = Name_ID, autoincrement = TRUE) |>
    dm::dm_add_fk(table = table2, columns = Name_ID, ref_table = table1, ref_columns = Name_ID)

  # Create tables to add to the DM
  table1 <- data.frame(Name_ID = as.integer(1:4),
                       Name = c("John", "Paul", "George", "Ringo"))
  table2 <- data.frame(Name_ID = as.integer(1:4),
                       Occupation = c("Lead singer", "Bassist", "Guitarist", "Drummer"))
  tables_to_add <- list(table1 = table1, table2 = table2)
  upload_schema_and_simple_tables(.dm = DM,
                                  simple_tables = tables_to_add,
                                  conn = conn,
                                  drop_db_tables = TRUE)

  # Get the data model from the database
  expect_equal(DBI::dbListTables(conn), c("table1", "table2"))
  expect_equal(DBI::dbReadTable(conn, "table1"), table1)
  expect_equal(DBI::dbReadTable(conn, "table2"), table2)

  # Try a table that should fail due to a bad primary key
  george_martin <- data.frame(Name_ID = as.integer(5), Occupation = "Producer")
  tables_to_add <- list(table1 = table1, table2 = george_martin)
  upload_schema_and_simple_tables(.dm = DM,
                                  simple_tables = tables_to_add,
                                  conn = conn,
                                  drop_db_tables = TRUE) |>
    expect_error(regexp = "In index: 2.")
})
