test_that("load_schema_table() works as expected", {
  # There are too many path dependencies to work on CI.
  skip_on_ci()
  skip_on_cran()
  st <- load_schema_table(version = "v2.0")
  expect_true("Table" %in% colnames(st))
  expect_true("colname" %in% colnames(st))
  expect_true("coldatatype" %in% colnames(st))
  expect_true("fk.table" %in% colnames(st))
  expect_true("fk.colname" %in% colnames(st))
})


test_that("schema_dm() works as expected", {
  # There are too many path dependencies to work on CI.
  skip_on_ci()
  skip_on_cran()
  clpfu_dm <- load_schema_table(version = "v2.0") |>
    schema_dm()
  clpfu_dm |>
    dm::dm_get_all_fks() |>
    nrow() |>
    expect_gt(5)
  clpfu_dm |>
    dm::dm_get_all_pks() |>
    nrow() |>
    expect_gt(5)
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
  skip_on_cran()
  simple_tables <- load_simple_tables(version = "v2.0")
  expect_true("Year" %in% names(simple_tables))
  expect_true("Method" %in% names(simple_tables))
})


test_that("pl_upload_schema_and_simple_tables() works as expected", {
  skip_on_ci()
  skip_on_cran()
  conn <- DBI::dbConnect(drv = RPostgres::Postgres(),
                         dbname = "unit_testing",
                         host = "eviz.cs.calvin.edu",
                         port = 5432,
                         user = "postgres")
  on.exit(DBI::dbDisconnect(conn))

  # Build the data model remotely
  PFUPipelineTools:::upload_beatles(conn)

  # Get the tables from the database
  tables <- DBI::dbListTables(conn)
  for (table_name in c("Member", "Role")) {
    expect_true(table_name %in% tables)
  }
  members <- data.frame(MemberID = 1:4,
                        Member = c("John Lennon", "Paul McCartney", "George Harrison", "Ringo Starr"))
  roles <- data.frame(RoleID = 1:4,
                      Role = c("Lead singer", "Bassist", "Guitarist", "Drummer"))
  expect_equal(DBI::dbReadTable(conn, "Member"), members)
  expect_equal(DBI::dbReadTable(conn, "Role"), roles)
})


test_that("pl_upsert() works with auto-incrementing columns", {
  skip_on_ci()
  skip_on_cran()
  conn <- DBI::dbConnect(drv = RPostgres::Postgres(),
                         dbname = "unit_testing",
                         host = "eviz.cs.calvin.edu",
                         port = 5432,
                         user = "postgres")
  on.exit(DBI::dbDisconnect(conn))

  # Build the data model remotely
  PFUPipelineTools:::upload_beatles(conn)

  # Add Stu Sutcliff
  stu_sutcliff_member <- data.frame(MemberID = as.integer(6),
                                    Member = "Stu Sutcliff")
  pl_upsert(stu_sutcliff_member, "Member", conn, in_place = TRUE)
})

