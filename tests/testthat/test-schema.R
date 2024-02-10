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
  mems <- matrix(nrow = 0, ncol = 2, dimnames = list(c(), c("Name_ID", "Name"))) |>
    as.data.frame() |>
    dplyr::mutate(
      Name_ID = as.integer(Name_ID),
      Name = as.character(Name)
    )
  rls <- matrix(nrow = 0, ncol = 2, dimnames = list(c(), c("Name_ID", "Role"))) |>
    as.data.frame() |>
    dplyr::mutate(
      Name_ID = as.integer(Name_ID),
      Role = as.character(Role)
    )

  DM <- list(Members = mems, Roles = rls) |>
    dm::as_dm() |>
    dm::dm_add_pk(table = Members, columns = Name_ID, autoincrement = TRUE) |>
    dm::dm_add_pk(table = Roles, columns = Name_ID, autoincrement = TRUE) |>
    dm::dm_add_fk(table = Roles, columns = Name_ID, ref_table = Members, ref_columns = Name_ID)

  # Create tables to add to the DM
  members <- data.frame(Name_ID = as.integer(1:4),
                       Name = c("John", "Paul", "George", "Ringo"))
  roles <- data.frame(Name_ID = as.integer(1:4),
                       Role = c("Lead singer", "Bassist", "Guitarist", "Drummer"))
  tables_to_add <- list(Members = members, Roles = roles)
  upload_schema_and_simple_tables(.dm = DM,
                                  simple_tables = tables_to_add,
                                  conn = conn,
                                  drop_db_tables = TRUE)

  # Get the data model from the database
  tables <- DBI::dbListTables(conn)
  for (table_name in c("Members", "Roles")) {
    expect_true(table_name %in% tables)
  }
  expect_equal(DBI::dbReadTable(conn, "Members"), members)
  expect_equal(DBI::dbReadTable(conn, "Roles"), roles)

  # Try to upload more data.
  george_martin_member <- data.frame(Name_ID = as.integer(5),
                                     Name = "George Martin")
  george_martin_role <- data.frame(Name_ID = as.integer(5),
                                   Role = "Producer")
  # This should fail due to a bad primary key.
  # There is no Name_ID = 5 in the Members table.
  dplyr::tbl(conn, "Roles") |>
    dplyr::rows_upsert(george_martin_role,
                       by = "Name_ID",
                       copy = TRUE,
                       in_place = TRUE) |>
    expect_error("Can't modify database table")

  # First add George Martin to the Members table.
  dplyr::tbl(conn, "Members") |>
    dplyr::rows_upsert(george_martin_member,
                       by = "Name_ID",
                       copy = TRUE,
                       in_place = TRUE)

  # Now add to the Roles table
  dplyr::tbl(conn, "Roles") |>
    dplyr::rows_upsert(george_martin_role,
                       by = "Name_ID",
                       copy = TRUE,
                       in_place = TRUE)
})
