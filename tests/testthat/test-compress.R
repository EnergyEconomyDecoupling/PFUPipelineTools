test_that("install_compress_function() and remove_compress_function() both work", {

  skip_on_ci()
  skip_on_cran()

  conn <- DBI::dbConnect(drv = RPostgres::Postgres(),
                         dbname = "unit_testing",
                         host = "mexer.site",
                         port = 5432,
                         user = "mkh2")
  on.exit(DBI::dbDisconnect(conn))

  install_compress_function(conn = conn) |>
    expect_equal(0)

  remove_compress_function(conn = conn) |>
    expect_equal(0)
})


test_that("Compression works maunally and with pl_upsert()", {

  skip_on_ci()
  skip_on_cran()

  conn <- DBI::dbConnect(drv = RPostgres::Postgres(),
                         dbname = "unit_testing",
                         host = "mexer.site",
                         port = 5432,
                         user = "mkh2")
  on.exit(DBI::dbDisconnect(conn))

  db_table_name <- "PLUpsertTest"
  if (db_table_name %in% DBI::dbListTables(conn)) {
    DBI::dbRemoveTable(conn, db_table_name)
  }
  db_table <- tibble::tribble(
    ~ValidFromVersion, ~ValidToVersion, ~Country, ~Year, ~val,
    1L, 1L, 1L, 1967L, 1,
    1L, 1L, 2L, 1967L, 2,
    2L, 2L, 1L, 1967L, 1,
    2L, 2L, 2L, 1967L, 2,
    2L, 2L, 1L, 1968L, 5,
    2L, 2L, 2L, 1968L, 6,
    3L, 3L, 1L, 1968L, 5,
    3L, 3L, 2L, 1968L, 6,
    3L, 3L, 3L, 2000L, 7,
    4L, 10L, 3L, 2000L, 8) |>
    as.data.frame()
  db_table_empty <- db_table[0, ]

  # Version table with a VersionID column and a Version column.
  version_table_name <- "VersionTablePLUpsertTest"
  if (version_table_name %in% DBI::dbListTables(conn)) {
    DBI::dbRemoveTable(conn, version_table_name)
  }
  version_table <- data.frame(VersionID = 1:10,
                              Version = paste0("v", 1:10))
  version_table_empty <- version_table[0, ]

  # Country table with a CountryID column
  country_table_name <- "CountryTablePLUpsertTest"
  if (country_table_name %in% DBI::dbListTables(conn)) {
    DBI::dbRemoveTable(conn, country_table_name)
  }
  country_table <- data.frame(CountryID = c(1, 2, 3),
                              Country = c("USA", "ZAF", "GHA"))
  country_table_empty <- country_table[0, ]

  # Build the data model
  DM <- list(db_table_empty, version_table_empty, country_table_empty) |>
    magrittr::set_names(c(db_table_name, version_table_name, country_table_name)) |>
    dm::as_dm() |>
    # Add primary keys
    dm::dm_add_pk({{db_table_name}}, c(ValidFromVersion,
                                       ValidToVersion,
                                       Country,
                                       Year)) |>
    dm::dm_add_pk({{version_table_name}}, VersionID) |>
    dm::dm_add_pk({{country_table_name}}, CountryID) |>
    # Add foreign keys
    dm::dm_add_fk(table = PLUpsertTest,
                  columns = ValidFromVersion,
                  ref_table = VersionTablePLUpsertTest,
                  ref_columns = VersionID) |>
    dm::dm_add_fk(table = PLUpsertTest,
                  columns = ValidToVersion,
                  ref_table = VersionTablePLUpsertTest,
                  ref_columns = VersionID) |>
    dm::dm_add_fk(table = PLUpsertTest,
                  columns = Country,
                  ref_table = CountryTablePLUpsertTest,
                  ref_columns = CountryID)

  dm::copy_dm_to(dest = conn, dm = DM, temporary = FALSE)

  # Get these things here to reduce download time below.
  schema <- schema_from_conn(conn = conn)
  fk_parent_tables <- get_all_fk_tables(conn = conn, schema = schema)

  # Upload tables
  hash_vt <- version_table |>
    pl_upsert(db_table_name = version_table_name,
              conn = conn,
              in_place = TRUE)
  hash_ct <- country_table |>
    pl_upsert(db_table_name = country_table_name,
              conn = conn,
              in_place = TRUE)
  hash_dbt <- db_table |>
    pl_upsert(db_table_name = db_table_name,
              conn = conn,
              in_place = TRUE)

  # Make sure we get the same table back before calling compress()
  expected_db_table <- tibble::tribble(
    ~ValidFromVersion, ~ValidToVersion, ~Country, ~Year, ~val,
    "v1", "v1", "USA", 1967L, 1,
    "v1", "v1", "ZAF", 1967L, 2,
    "v2", "v2", "USA", 1967L, 1,
    "v2", "v2", "ZAF", 1967L, 2,
    "v2", "v2", "USA", 1968L, 5,
    "v2", "v2", "ZAF", 1968L, 6,
    "v3", "v3", "USA", 1968L, 5,
    "v3", "v3", "ZAF", 1968L, 6,
    "v3", "v3", "GHA", 2000L, 7,
    "v4", "v10", "GHA", 2000L, 8)

  pl_filter_collect(db_table_name = db_table_name,
                    collect = TRUE,
                    conn = conn,
                    schema = schema,
                    fk_parent_tables = fk_parent_tables) |>
    expect_equal(expected_db_table)

  #### Compress and compare to expected.
  install_compress_function(conn = conn) |>
    expect_equal(0)

  expected_compressed_table <- tibble::tribble(
    ~ValidFromVersion, ~ValidToVersion, ~Country, ~Year, ~val,
    # "v1", "v1", "USA", 1967L, 1,
    # "v2", "v2", "USA", 1967L, 1,
    "v1", "v2", "USA", 1967L, 1,
    # "v2", "v2", "USA", 1968L, 5,
    # "v3", "v3", "USA", 1968L, 5,
    "v2", "v3", "USA", 1968L, 5,
    # "v1", "v1", "ZAF", 1967L, 2,
    # "v2", "v2", "ZAF", 1967L, 2,
    "v1", "v2", "ZAF", 1967L, 2,
    # "v2", "v2", "ZAF", 1968L, 6,
    # "v3", "v3", "ZAF", 1968L, 6,
    "v2", "v3", "ZAF", 1968L, 6,
    "v3", "v3", "GHA", 2000L, 7,
    "v4", "v10", "GHA", 2000L, 8) |>
    dplyr::arrange(val)

  # Compress the db_table
  compress_rows(db_table_name = db_table_name, conn = conn) |>
    expect_equal(0)
  # Re-collect the table and compare
  pl_filter_collect(db_table_name = db_table_name,
                              collect = TRUE,
                              conn = conn,
                              schema = schema,
                              fk_parent_tables = fk_parent_tables) |>
    dplyr::arrange(val) |>
    expect_equal(expected_compressed_table)

  # Eliminate rows from db_table in preparation for compressing on upload.
  DBI::dbExecute(conn,
                 statement = paste0('DELETE FROM "', db_table_name, '";')) |>
    expect_equal(nrow(expected_compressed_table))
  # Now re-upload the data frame using compress = TRUE
  hash_cdbt <- db_table |>
    pl_upsert(db_table_name = db_table_name,
              conn = conn,
              schema = schema,
              fk_parent_tables = fk_parent_tables,
              in_place = TRUE,
              compress = TRUE)
  # Make sure the compression happened as expected
  # and gives the same results as before.
  pl_filter_collect(db_table_name = db_table_name,
                    collect = TRUE,
                    conn = conn,
                    schema = schema,
                    fk_parent_tables = fk_parent_tables) |>
    dplyr::arrange(val) |>
    expect_equal(expected_compressed_table)

  # Eliminate rows from db_table in preparation for compressing
  # an incompressible table, which should be successful but have no effect.
  # Also, this time try the rounding function.
  incompressible_db_table <- tibble::tribble(
    ~ValidFromVersion, ~ValidToVersion, ~Country, ~Year, ~val,
    "v1", "v1", "USA", 1967L, 1.001, # Should become 1
    "v1", "v1", "ZAF", 1967L, 1.96,  # Should become 2
    "v2", "v2", "USA", 1968L, 5.001, # Should become 5
    "v2", "v2", "ZAF", 1968L, 5.955, # Should become 6
    "v3", "v3", "GHA", 2000L, 7.02,  # Should become 7
    "v4", "v10", "GHA", 2000L, 7.99)  # Should become 8

  expected_incompressible_db_table <- tibble::tribble(
    ~ValidFromVersion, ~ValidToVersion, ~Country, ~Year, ~val,
    "v1", "v1", "USA", 1967L, 1,
    "v1", "v1", "ZAF", 1967L, 2,
    "v2", "v2", "USA", 1968L, 5,
    "v2", "v2", "ZAF", 1968L, 6,
    "v3", "v3", "GHA", 2000L, 7,
    "v4", "v10", "GHA", 2000L, 8)

  DBI::dbExecute(conn,
                 statement = paste0('DELETE FROM "', db_table_name, '";')) |>
    expect_equal(nrow(expected_compressed_table))
  # Now re-upload only unique rows in the data frame using compress = TRUE
  hash_cdbt_incompressible <- db_table |>
    dplyr::slice(c(1, 2, 5, 6, 9, 10)) |>
    pl_upsert(db_table_name = db_table_name,
              conn = conn,
              schema = schema,
              fk_parent_tables = fk_parent_tables,
              in_place = TRUE,
              round_double_columns = TRUE,
              digits = 2,
              compress = TRUE)
  # Make sure no compression happened.
  pl_filter_collect(db_table_name = db_table_name,
                    collect = TRUE,
                    conn = conn,
                    schema = schema,
                    fk_parent_tables = fk_parent_tables) |>
    dplyr::arrange(val) |>
    expect_equal(expected_incompressible_db_table)

  # Clean up after ourselves
  if (db_table_name %in% DBI::dbListTables(conn)) {
    DBI::dbRemoveTable(conn, db_table_name)
  }
  if (version_table_name %in% DBI::dbListTables(conn)) {
    DBI::dbRemoveTable(conn, version_table_name)
  }
  if (country_table_name %in% DBI::dbListTables(conn)) {
    DBI::dbRemoveTable(conn, country_table_name)
  }

  remove_compress_function(conn = conn) |>
    expect_equal(0)
})
