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


test_that("load_fk_tables() works as expected", {
  skip_on_ci()
  skip_on_cran()
  simple_tables <- load_fk_tables(version = "v2.0")
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
                         user = "mkh2")
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
  pl_upsert(memberrole,
            conn = conn,
            db_table_name = "MemberRole",
            in_place = TRUE)

  # Try to upload more data for the fifth Beatle.
  george_martin_member <- data.frame(MemberID = as.integer(5),
                                     Member = "George Martin")
  george_martin_memberrole <- data.frame(Member = as.integer(5),
                                         Role = as.integer(5))
  producer_role <- data.frame(RoleID = as.integer(5),
                              Role = "Producer")
  # Add George Martin as a member, the fifth Beatle
  pl_upsert(george_martin_member,
            conn = conn,
            db_table_name = "Member",
            in_place = TRUE)
  # Now try to add George Martin to the MemberRole table.
  # This should fail due to a bad primary key.
  # There is no RoleID = 5 in the Role table.
  pl_upsert(george_martin_memberrole,
            conn = conn,
            db_table_name =  "MemberRole",
            in_place = TRUE) |>
    expect_error('insert or update on table "MemberRole" violates foreign key constraint "MemberRole_Role_fkey"')
  # Instead, add the Producer role to the Role table, so that the Producer role pk will be available
  pl_upsert(producer_role,
            conn = conn,
            db_table_name = "Role",
            in_place = TRUE)
  # Now add George Martin to the MemberRole table
  pl_upsert(george_martin_memberrole,
            conn = conn,
            db_table_name = "MemberRole",
            in_place = TRUE)
  memberrole_tbl <- dplyr::tbl(conn, "MemberRole") |>
    dplyr::collect()
  expect_equal(nrow(memberrole_tbl), 5)
  expect_equal(memberrole_tbl$Role, 1:5)

  # Add a sixth role, Producer Extraordinaire
  prodext <- data.frame(RoleID = as.integer(6),
                        Role = "Producer Extraordinaire")
  pl_upsert(prodext,
            conn = conn,
            db_table_name =  "Role",
            in_place = TRUE)

  # Try to upsert with "George Martin" in the Member column.
  # This should decode "George Martin" into the Member_ID of 5 during the upsert.
  # Then change the Role to "Producer Extraordinaire"
  george_martin_role_name <- data.frame(Member = "George Martin",
                                        Role = "Producer Extraordinaire")
  pl_upsert(george_martin_role_name,
            conn = conn,
            db_table_name = "MemberRole",
            in_place = TRUE)
  # Check that George Martin is now Producer Extraordinaire
  # and in the Roles table has Member_ID of 5.
  new_memberrole <- dplyr::tbl(conn, "MemberRole") |>
    dplyr::collect()
  new_memberrole |>
    dplyr::filter(Member == 5) |>
    magrittr::extract2("Role") |>
    expect_equal(6)

  # Clean up after ourselves
  # Get rid of the MemberRole table first, because the other depend on it
  clean_up_beatles(conn)
})


test_that("encode_fks() works with re-routed foreign keys", {
  skip_on_ci()
  skip_on_cran()
  conn <- DBI::dbConnect(drv = RPostgres::Postgres(),
                         dbname = "unit_testing",
                         host = "eviz.cs.calvin.edu",
                         port = 5432,
                         user = "mkh2")
  on.exit(DBI::dbDisconnect(conn))
  # Get rid of tables before we start
  if (DBI::dbExistsTable(conn, "TestUpsertTable")) {
    DBI::dbRemoveTable(conn, "TestUpsertTable")
  }
  if (DBI::dbExistsTable(conn, "ECCStage")) {
    DBI::dbRemoveTable(conn, "ECCStage")
  }

  # Build the data model remotely
  TestUpsertTable <- data.frame(LastStage = as.integer(c(1, 2, 3)),
                                Value = c(42, 43, 44))
  TestUpsertTable2 <- data.frame(LastStage = c("Primary", "Final", "Useful"),
                                 Value = c(42, 43, 44))
  ECCStage <- data.frame(ECCStageID = as.integer(c(1, 2, 3)),
                         ECCStage = c("Primary", "Final", "Useful"))
  DM <- list(TestUpsertTable = TestUpsertTable[0, ],
             ECCStage = ECCStage) |>
    dm::as_dm() |>
    dm::dm_add_pk(TestUpsertTable, LastStage) |>
    dm::dm_add_pk(ECCStage, ECCStageID) |>
    dm::dm_add_fk(TestUpsertTable, LastStage, ECCStage, ECCStageID)
  dm::copy_dm_to(conn, DM, temporary = FALSE, set_key_constraints = TRUE)
  Sys.sleep(0.5) # Make sure the database has time to put everything in place.
  pl_upsert(TestUpsertTable,
            conn = conn,
            db_table_name = "TestUpsertTable",
            in_place = TRUE)
  # Get the table to make sure it worked
  retrieved <- DBI::dbReadTable(conn, "TestUpsertTable")
  expect_equal(retrieved, TestUpsertTable)
  decoded <- pl_filter_collect("TestUpsertTable", last_stages = NULL, conn = conn, collect = TRUE)
  expect_equal(decoded, TestUpsertTable2, ignore_attr = TRUE)
  # Now try with decoding
  pl_upsert(TestUpsertTable2,
            conn = conn,
            db_table_name = "TestUpsertTable",
            in_place = TRUE,
            encode_fks = TRUE)
  Sys.sleep(0.5) # Make sure the database has time to put everything in place.
  retrieved2 <- DBI::dbReadTable(conn, "TestUpsertTable")
  expect_equal(retrieved2, TestUpsertTable)
  Sys.sleep(0.5) # Make sure the database has time to put everything in place.
  # Retrieve with decoding
  decoded2 <- pl_filter_collect("TestUpsertTable", last_stages = NULL, conn = conn, collect = TRUE)
  expect_equal(decoded2, TestUpsertTable2, ignore_attr = TRUE)

  # Clean up after ourselves
  DBI::dbRemoveTable(conn, "TestUpsertTable")
  DBI::dbRemoveTable(conn, "ECCStage")
})


test_that("decode_fks() works as expected", {
  skip_on_ci()
  skip_on_cran()
  conn <- DBI::dbConnect(drv = RPostgres::Postgres(),
                         dbname = "unit_testing",
                         host = "eviz.cs.calvin.edu",
                         port = 5432,
                         user = "mkh2")
  on.exit(DBI::dbDisconnect(conn))

  # Build the data model remotely
  PFUPipelineTools:::upload_beatles(conn)

  # Add Stu Sutcliff
  stu_sutcliff_member <- data.frame(MemberID = as.integer(5),
                                    Member = "Stu Sutcliff")
  pl_upsert(stu_sutcliff_member,
            conn = conn,
            db_table_name = "Member",
            in_place = TRUE)
  # Now add to MemberRole table
  stu_sutcliff_memberrole <- data.frame(Member = "Stu Sutcliff",
                                        Role = "Bassist")
  pl_upsert(stu_sutcliff_memberrole,
            conn = conn,
            db_table_name = "MemberRole",
            in_place = TRUE)
  memberrole_tbl <- dplyr::tbl(conn, "MemberRole") |>
    dplyr::collect() |>
    as.data.frame()
  memberrole_tbl |>
    expect_equal(data.frame(Member = 5, Role = 2))

  # Check decoding the old-fashioned way
  expected <- data.frame(Member = "Stu Sutcliff",
                         Role = "Bassist")
  schema <- schema_from_conn(conn)
  Sys.sleep(0.5) # Make sure the database has time to put everything in place.
  fk_parent_tables <- get_all_fk_tables(conn = conn, schema = schema)
  memberrole_tbl |>
    decode_fks(db_table_name = "MemberRole",
               schema = schema,
               fk_parent_tables = fk_parent_tables) |>
    expect_equal(expected)


  # Check that decoding works when .df is NULL, a much simpler way.
  decoded <- decode_fks(db_table_name = "MemberRole",
                        conn = conn,
                        collect = TRUE)
  expect_equal(decoded, expected)

  # Clean up after ourselves
  PFUPipelineTools:::clean_up_beatles(conn)
})


test_that("decode_fks() and encode_fks() work with tbls", {
  skip_on_ci()
  skip_on_cran()
  conn <- DBI::dbConnect(drv = RPostgres::Postgres(),
                         dbname = "unit_testing",
                         host = "eviz.cs.calvin.edu",
                         port = 5432,
                         user = "mkh2")
  on.exit(DBI::dbDisconnect(conn))

  # Build the data model remotely
  PFUPipelineTools:::upload_beatles(conn)

  memberrole <- data.frame(Member = as.integer(1:4),
                           Role = as.integer(1:4))
  memberrole |>
    pl_upsert(conn = conn,
              db_table_name = "MemberRole",
              in_place = TRUE)
  schema <- schema_from_conn(conn)
  # This next call should download MemberRole as a tbl, because
  # (a) .df is not specified and
  # (b) collect = FALSE (the default)
  decoded_tbl <- decode_fks(db_table_name = "MemberRole",
                            conn = conn,
                            schema = schema)
  expect_true(dplyr::is.tbl(decoded_tbl))
  expect_equal(dplyr::collect(decoded_tbl),
               tibble::tribble(~Member, ~Role,
                               "John Lennon", "Lead singer",
                               "Paul McCartney", "Bassist",
                               "George Harrison", "Guitarist",
                               "Ringo Starr", "Drummer"))

  # At this point, decoded_tbl is a tbl.
  # Try to encode it
  re_encoded_tbl <- encode_fks(decoded_tbl,
             db_table_name = "MemberRole",
             conn = conn)
  expect_true(dplyr::is.tbl(re_encoded_tbl))
  expect_equal(dplyr::collect(re_encoded_tbl) |>
                 dplyr::arrange(Member),
               tibble::tibble(Member = as.integer(1:4),
                              Role = as.integer(1:4)))

  # Clean up after ourselves
  PFUPipelineTools:::clean_up_beatles(conn)
})


test_that("pl_collect_from_hash() decodes correctly", {
  skip_on_ci()
  skip_on_cran()
  conn <- DBI::dbConnect(drv = RPostgres::Postgres(),
                         dbname = "unit_testing",
                         host = "eviz.cs.calvin.edu",
                         port = 5432,
                         user = "mkh2")
  on.exit(DBI::dbDisconnect(conn))

  # Build the data model remotely
  PFUPipelineTools:::upload_beatles(conn)

  # Create the MemberRole table
  memberrole <- data.frame(Member = as.integer(1:4),
                           Role = as.integer(1:4))
  mr <- pl_upsert(memberrole,
                  conn = conn,
                  db_table_name = "MemberRole",
                  in_place = TRUE)

  res <- pl_collect_from_hash(hashed_table = mr, conn = conn, decode_fks = TRUE)
  expectedMR <- tibble::tribble(~Member, ~Role,
                                "John Lennon", "Lead singer",
                                "Paul McCartney", "Bassist",
                                "George Harrison", "Guitarist",
                                "Ringo Starr", "Drummer")
  expect_equal(res, expectedMR)

  # Try to upsert when a role is incorrect
  wrong_members <- tibble::tribble(~Member, ~Role,
                                   "Pete Best", "Bassist",
                                   "Stu Sutcliff", "Drummer")
  pl_upsert(wrong_members, conn = conn, db_table_name = "MemberRole", in_place = TRUE) |>
    expect_error(regexp = "unable to convert the following entries to foreign keys")

  # Clean up after ourselves
  PFUPipelineTools:::clean_up_beatles(conn)
})


test_that("pl_upsert() works for zero matrices", {
  skip_on_ci()
  skip_on_cran()
  conn <- DBI::dbConnect(drv = RPostgres::Postgres(),
                         dbname = "unit_testing",
                         host = "eviz.cs.calvin.edu",
                         port = 5432,
                         user = "mkh2")
  on.exit(DBI::dbDisconnect(conn))

  # Start with a fresh slate
  if (DBI::dbExistsTable(conn = conn, name = "testzeromatrix")) {
    DBI::dbRemoveTable(conn = conn, name = "testzeromatrix")
  }

  # Create data model
  dm <- list(testzeromatrix = data.frame(matname = "zerom",
                                         i = as.integer(1),
                                         j = as.integer(1),
                                         value = 3.1415926) |>
               # Delete all rows, but keep names and column types
               dplyr::filter(FALSE)) |>
    dm::new_dm() |>
    dm::dm_add_pk(testzeromatrix, columns = c(matname, i, j))
  dm::copy_dm_to(conn, dm = dm, temporary = FALSE)
  # Create index map
  index_map <- list(row = data.frame(IndexID = as.integer(1:3),
                                     Index = c("r1", "r2", "r3")),
                    col = data.frame(IndexID = as.integer(1:2),
                                     Index = c("c1", "c2")))

  # Create a zero matrix
  zerom <- matrix(c(0, 0,
                    0, 0,
                    0, 0), nrow = 3, dimnames = list(c("r1", "r2", "r3"), c("c1", "c2"))) |>
    matsbyname::setrowtype("row") |> matsbyname::setcoltype("col")
  # Create a matsindf data frame
  midf <- tibble::tibble(matname = c("zerom1", "zerom2"),
                         matval = list(zerom, zerom))
  no_rows <- midf |>
    pl_upsert(conn = conn,
              db_table_name = "testzeromatrix",
              index_map = index_map)
  # Check that there are no rows in the hashed table
  expect_equal(nrow(no_rows), 0)
  # Check that there are no rows in the table
  should_be_no_rows <- DBI::dbReadTable(conn, name = "testzeromatrix")
  expect_equal(nrow(should_be_no_rows), 0)

  # Now upsert zerom while preserving rows
  twelve_rows <- midf |>
    pl_upsert(conn = conn,
              db_table_name = "testzeromatrix",
              index_map = index_map,
              in_place = TRUE,
              retain_zero_structure = TRUE)
  # The hash should come back with 1 row
  expect_equal(nrow(twelve_rows), 1)
  # Check that there are twelve rows in the table
  should_be_twelve_rows <- DBI::dbReadTable(conn, name = "testzeromatrix")
  expect_equal(nrow(should_be_twelve_rows), 12)

  # Now try to use pl_filter_collect() to get the data.
  rctypes <- tibble::tribble(~matname, ~rowtype, ~coltype,
                             "zerom1", "row", "col",
                             "zerom2", "row", "col")



  # The following should give zero matrices with
  # row and column names
  filter_collected <- pl_filter_collect(db_table_name = "testzeromatrix",
                                        conn = conn,
                                        collect = TRUE,
                                        index_map = index_map,
                                        rctypes = rctypes)
  expect_equal(nrow(filter_collected), 1)
  expect_equal(colnames(filter_collected), c("zerom1", "zerom2"))
  expect_equal(nrow(filter_collected$zerom1[[1]]), 3)
  expect_equal(ncol(filter_collected$zerom1[[1]]), 2)
  expect_equal(nrow(filter_collected$zerom2[[1]]), 3)
  expect_equal(ncol(filter_collected$zerom2[[1]]), 2)
  expect_true(matsbyname::iszero_byname(filter_collected$zerom1[[1]]))
  expect_true(matsbyname::iszero_byname(filter_collected$zerom2[[1]]))
  expect_equal(rownames(filter_collected$zerom1[[1]]), c("r1", "r2", "r3"))
  expect_equal(colnames(filter_collected$zerom1[[1]]), c("c1", "c2"))
  expect_equal(rownames(filter_collected$zerom2[[1]]), c("r1", "r2", "r3"))
  expect_equal(colnames(filter_collected$zerom2[[1]]), c("c1", "c2"))

  # Clean up after ourselves
  DBI::dbRemoveTable(conn = conn, name = "testzeromatrix")
})
