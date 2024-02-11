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


test_that("pl_upload_schema_and_simple_tables() works as expected", {
  skip_on_ci()
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
  for (table_name in c("Members", "Roles")) {
    expect_true(table_name %in% tables)
  }
  members <- data.frame(Member_ID = as.integer(1:4),
                        Member = c("John Lennon", "Paul McCartney", "George Harrison", "Ringo Starr"))
  roles <- data.frame(Member_ID = as.integer(1:4),
                      Role = c("Lead singer", "Bassist", "Guitarist", "Drummer"))
  expect_equal(DBI::dbReadTable(conn, "Members"), members)
  expect_equal(DBI::dbReadTable(conn, "Roles"), roles)
})



test_that("pl_upsert() works as expected", {
  skip_on_ci()
  conn <- DBI::dbConnect(drv = RPostgres::Postgres(),
                         dbname = "unit_testing",
                         host = "eviz.cs.calvin.edu",
                         port = 5432,
                         user = "postgres")
  on.exit(DBI::dbDisconnect(conn))

  # Build the data model remotely
  PFUPipelineTools:::upload_beatles(conn)

  # Try to upload more data for the fifth Beatle.
  george_martin_member <- data.frame(Member_ID = as.integer(5),
                                     Member = "George Martin")
  george_martin_role <- data.frame(Member_ID = as.integer(5),
                                   Role = "Producer")
  # This should fail due to a bad primary key.
  # There is no Name_ID = 5 in the Members table.
  pl_upsert(george_martin_role, "Roles", conn) |>
    expect_error('insert or update on table "Roles" violates foreign key constraint')
  # Instead, add George Martin to the Members table so that his primary key will be available.
  pl_upsert(george_martin_member, "Members", conn)
  # Then Now add to the Roles table
  pl_upsert(george_martin_role, "Roles", conn)
  roles_tbl <- dplyr::tbl(conn, "Roles") |>
    dplyr::collect()
  expect_equal(nrow(roles_tbl), 5)
  expect_equal(roles_tbl$Role, c("Lead singer", "Bassist", "Guitarist", "Drummer", "Producer"))


  # Try to upsert with "George Martin" in the Member column.
  # This should decode "George Martin" into the Member_ID of 5 during the upsert.
  # Then change the Role to "Producer Extraordinaire"
  george_martin_role_name <- data.frame(Member_ID = "George Martin",
                                        Role = "Producer Extraordinaire")
  pl_upsert(george_martin_role_name, "Roles", conn)
  # Check that George Martin is now Producer Extraordinaire
})
