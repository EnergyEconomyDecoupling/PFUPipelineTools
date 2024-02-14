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


test_that("pl_upsert() works as expected", {
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

  # Add a MemberRole table, linking members and roles
  memberrole <- data.frame(MemberRoleID = 1:4, Member = 1:4, Role = 1:4)
  pl_upsert(memberrole, db_table_name = "MemberRole", conn = conn, in_place = TRUE)


  # Try to upload more data for the fifth Beatle
  george_martin_member <- data.frame(MemberID = as.integer(5),
                                     Member = "George Martin")
  # Add George Martin as the fifth Beatle in the Member table
  pl_upsert(george_martin_member, "Member", conn, in_place = TRUE)

  # But now try to add George Martin to the MemberRole table.
  # This should fail due to a bad foreign key.
  # There is no RoleID = 5 in the Roles table.
  george_martin_memberrole <- data.frame(MemberRoleID = as.integer(5),
                                         Member = as.integer(5),
                                         Role = as.integer(5))
  pl_upsert(george_martin_memberrole, "MemberRole", conn, in_place = TRUE) |>
    expect_error(regexp = 'insert or update on table "MemberRole" violates foreign key constraint "MemberRole_Role_fkey"')

  # To fix the problem, add a Producer role to the Role table
  producer_role <- data.frame(RoleID = as.integer(5),
                              Role = "Producer")
  pl_upsert(producer_role, "Role", conn, in_place = TRUE)
  roles_tbl <- dplyr::tbl(conn, "Role") |>
    dplyr::collect()
  expect_equal(nrow(roles_tbl), 5)
  expect_equal(roles_tbl[["Role"]], c("Lead singer", "Bassist", "Guitarist", "Drummer", "Producer"))

  # Update the Role table with Producer Extraordinaire.
  prod_extra <- data.frame(RoleID = as.integer(6),
                           Role = "Producer Extraordinaire")
  pl_upsert(prod_extra, db_table_name = "Role", conn = conn, in_place = TRUE)
  roles_tbl <- dplyr::tbl(conn, "Role") |>
    dplyr::collect()
  expect_equal(nrow(roles_tbl), 6)
  expect_equal(roles_tbl$Role, c("Lead singer", "Bassist", "Guitarist", "Drummer", "Producer", "Producer Extraordinaire"))


  # This should decode "George Martin" into the MemberID of 5 during the upsert.
  # Then change the Role to "Producer Extraordinaire"
  george_martin_role_names <- data.frame(MemberRoleID = as.integer(5),
                                         Member = "George Martin",
                                         Role = "Producer Extraordinaire")
  pl_upsert(george_martin_role_names, "MemberRole", conn, in_place = TRUE)
  # Check that George Martin is now Producer Extraordinaire
  # and in the Roles table has Member_ID of 5.
  new_roles <- dplyr::tbl(conn, "MemberRole") |>
    dplyr::collect()
  new_roles |>
    dplyr::filter(Member == 5) |>
    magrittr::extract2("Role") |>
    expect_equal(6)

  # Now try to add Pete Best, this time using pl_upsert with schema and
  # fk_parent_tables pre-computed
  schema <- dm::dm_from_con(conn, learn_keys = TRUE)
  fk_parent_tables <- get_all_fk_tables(conn, schema)
  pete_best <- data.frame(MemberID = 6, Member = "Pete Best")
  # Using the old fk_parent_tables for now.
  # But it doesn't matter, because the upsert
  # to the Members table doesn't rely upon any foreign keys.
  pl_upsert(pete_best,
            db_table_name = "Member",
            conn = conn,
            in_place = TRUE,
            fk_parent_tables = fk_parent_tables)
  # Try to upsert Pete's role with the old fk_parent_tables.
  # This should fail.
  pete_best_memberrole <- data.frame(MemberRoleID = 6, Member = "Pete Best", Role = "Drummer")
  pl_upsert(pete_best_memberrole,
            db_table_name = "MemberRole",
            conn = conn,
            in_place = TRUE,
            schema = schema,
            fk_parent_tables = fk_parent_tables) |>
    expect_error('null value in column "Member" of relation "MemberRole" violates not-null constraint')
  # Get the new fk_parent_tables
  fk_parent_tables_new <- get_all_fk_tables(conn, schema)
  # The upsert should work with the new fk_parent_tables
  pl_upsert(pete_best_memberrole,
            db_table_name = "MemberRole",
            conn = conn,
            in_place = TRUE,
            schema = schema,
            fk_parent_tables = fk_parent_tables_new)
})
























