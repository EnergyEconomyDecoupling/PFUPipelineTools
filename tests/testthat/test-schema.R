test_that("load_schema_table() works as expected", {
  # There are too many path dependencies to work on CI.
  skip_on_ci()
  skip_on_cran()
  st <- load_schema_table(version = "v2.0")
  expect_true("Table" %in% colnames(st))
  expect_true("Colname" %in% colnames(st))
  expect_true("ColDataType" %in% colnames(st))
  expect_true("FKTable" %in% colnames(st))
  expect_true("FKColname" %in% colnames(st))
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
  # Test that we can have multiple primary keys
  clpfu_dm |>
    dm::dm_get_all_pks() |>
    dplyr::filter(table == "PSUT") |>
    magrittr::extract2("pk_col") |>
    unlist() |>
    length() |>
    expect_equal(11)
})


test_that("schema_dm() fails with unknown data type", {
  st <- tibble::tribble(~Table, ~Colname, ~IsPK, ~ColDataType, ~FKTable, ~FKColname,
                        "Country", "CountryID", TRUE, "bogus", "NA", "NA",
                        "Country", "Country", FALSE, "text", "NA", "NA",
                        "Country", "Description", FALSE, "text", "NA", "NA")
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

  # Check that data types are correct
  expect_true(is.integer(simple_tables$Country$CountryID))
  expect_true(is.character(simple_tables$Country$Country))
  expect_true(is.integer(simple_tables$Year$YearID))
  expect_true(is.integer(simple_tables$Year$Year))
  expect_true(is.integer(simple_tables$IncludesNEU$IncludesNEUID))
  expect_true(is.logical(simple_tables$IncludesNEU$IncludesNEU))
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

  # Create the MemberRole table
  memberrole <- data.frame(Member = as.integer(1:4),
                           Role = as.integer(1:4))
  pl_upsert(memberrole, "MemberRole", conn, in_place = TRUE)

  # Try to upload more data for the fifth Beatle.
  george_martin_member <- data.frame(MemberID = as.integer(5),
                                     Member = "George Martin")
  george_martin_memberrole <- data.frame(Member = as.integer(5),
                                         Role = as.integer(5))
  producer_role <- data.frame(RoleID = as.integer(5),
                              Role = "Producer")
  # Add George Martin as a member, the fifth Beatle
  pl_upsert(george_martin_member, "Member", in_place = TRUE, conn = conn)
  # Now try to add George Martin to the MemberRole table.
  # This should fail due to a bad primary key.
  # There is no RoleID = 5 in the Role table.
  pl_upsert(george_martin_memberrole, "MemberRole", in_place = TRUE, conn = conn) |>
    expect_error('insert or update on table "MemberRole" violates foreign key constraint "MemberRole_Role_fkey"')
  # Instead, add the Producer role to the Role table, so that the Producer role pk will be available
  pl_upsert(producer_role, "Role", in_place = TRUE, conn = conn)
  # Now add George Martin to the MemberRole table
  pl_upsert(george_martin_memberrole, "MemberRole", in_place = TRUE, conn)
  memberrole_tbl <- dplyr::tbl(conn, "MemberRole") |>
    dplyr::collect()
  expect_equal(nrow(memberrole_tbl), 5)
  expect_equal(memberrole_tbl$Role, 1:5)

  # Add a sixth role, Producer Extraordinaire
  prodext <- data.frame(RoleID = as.integer(6),
                        Role = "Producer Extraordinaire")
  pl_upsert(prodext, "Role", conn, in_place = TRUE)

  # Try to upsert with "George Martin" in the Member column.
  # This should decode "George Martin" into the Member_ID of 5 during the upsert.
  # Then change the Role to "Producer Extraordinaire"
  george_martin_role_name <- data.frame(Member = "George Martin",
                                        Role = "Producer Extraordinaire")
  pl_upsert(george_martin_role_name, "MemberRole", in_place = TRUE, conn = conn)
  # Check that George Martin is now Producer Extraordinaire
  # and in the Roles table has Member_ID of 5.
  new_memberrole <- dplyr::tbl(conn, "MemberRole") |>
    dplyr::collect()
  new_memberrole |>
    dplyr::filter(Member == 5) |>
    magrittr::extract2("Role") |>
    expect_equal(6)
})


test_that("decode_keys() works as expected", {
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
  stu_sutcliff_member <- data.frame(MemberID = as.integer(5),
                                    Member = "Stu Sutcliff")
  pl_upsert(stu_sutcliff_member, "Member", conn, in_place = TRUE)
  # Now add to MemberRole table
  stu_sutcliff_memberrole <- data.frame(Member = "Stu Sutcliff",
                                        Role = "Bassist")
  pl_upsert(stu_sutcliff_memberrole, "MemberRole", conn, in_place = TRUE)
  memberrole_tbl <- dplyr::tbl(conn, "MemberRole") |>
    dplyr::collect() |>
    as.data.frame()
  memberrole_tbl |>
    expect_equal(data.frame(Member = 5, Role = 2))

  # Check decoding
  schema <- dm::dm_from_con(con = conn, learn_keys = TRUE)
  fk_parent_tables <- get_all_fk_tables(conn = conn, schema = schema)
  memberrole_tbl |>
    decode_keys(db_table_name = "MemberRole",
                schema = schema,
                fk_parent_tables = fk_parent_tables) |>
    expect_equal(data.frame(Member = "Stu Sutcliff",
                            Role = "Bassist"))
})











