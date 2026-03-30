
test_that("pl_collect_from_hash() works as expected", {

  skip_on_ci()
  skip_on_cran()
  conn <- DBI::dbConnect(drv = RPostgres::Postgres(),
                         dbname = "unit_testing",
                         host = "mexer.site",
                         port = 5432,
                         user = "mkh2")
  on.exit(DBI::dbDisconnect(conn))

  # Make a data frame for testing.
  # This data frame should have some columns with only 1 value
  # to test the hash returned by pl_upsert()

  db_table_name <- "PLCollectFromHashTestTable"
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
  Sys.sleep(1) # Make sure the database has time to put everything in place.
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
                 PFUPipelineTools::hashed_table_colnames$nested_hash_colname))
  expected_hash <- "e4c5198aa04c376d9096111092794069"
  hash1 |>
    dplyr::select(dplyr::all_of(PFUPipelineTools::hashed_table_colnames$nested_hash_colname)) |>
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
    dplyr::select(dplyr::all_of(PFUPipelineTools::hashed_table_colnames$nested_hash_colname)) |>
    unlist() |>
    unname() |>
    expect_equal(expected_hash)

  # Do we round-trip successfully?
  result1 <- pl_collect_from_hash(hash1, conn = conn, decode_fks = FALSE)
  expect_equal(result1, tibble::as_tibble(test_table1))
  result2 <- pl_collect_from_hash(hash2, conn = conn, decode_fks = FALSE)
  expect_equal(result2, tibble::as_tibble(test_table2))

  # Can we retain the table name column?
  result1r <- pl_collect_from_hash(hash1,
                                   conn = conn,
                                   decode_fks = FALSE,
                                   retain_table_name_col = TRUE)
  expect_true(PFUPipelineTools::hashed_table_colnames$db_table_name %in% colnames(result1r))
  expect_equal(result1r[[PFUPipelineTools::hashed_table_colnames$db_table_name]] |>
                 unique(),
               db_table_name)

  # Include key1 in the hash group cols,
  # to no effect, because key1 has only 1 unique value.
  hash3 <- pl_hash(test_table1,
                   table_name = db_table_name,
                   additional_hash_group_cols = "key1")
  expect_equal(nrow(hash3), 1)
  expect_equal(hash3$key1[[1]], 1)
  # Now try to retrieve the table with the hash
  result3 <- pl_collect_from_hash(hash3,
                                  conn = conn,
                                  decode_fks = TRUE,
                                  retain_table_name_col = FALSE)
  expect_equal(result3, test_table1, ignore_attr = TRUE)

  # Now include key2 in the hash group cols.
  # This will cause all rows to be hashed.
  hash4 <- pl_hash(test_table1,
                   table_name = db_table_name,
                   additional_hash_group_cols = "key2")
  expect_equal(nrow(hash4), 3)
  expect_equal(hash4$key2, c(1, 2, 3))
  # Now try to retrieve the table with the hash
  result4 <- pl_collect_from_hash(hash4,
                                  conn = conn,
                                  decode_fks = TRUE,
                                  retain_table_name_col = FALSE) |>
    dplyr::arrange(key2)
  expect_equal(result4, test_table1, ignore_attr = TRUE)

  # Now filter hash4 and retrieve.
  hash5 <- hash4 |>
    dplyr::filter(key2 %in% c(2,3))
  result5 <- pl_collect_from_hash(hash5,
                                  conn = conn,
                                  decode_fks = TRUE,
                                  retain_table_name_col = FALSE) |>
    dplyr::arrange(key2)
  expect_equal(result5, test_table1 |> dplyr::filter(key2 != 1), ignore_attr = TRUE)

  DBI::dbRemoveTable(conn, db_table_name)
})


test_that("passing filters in ... works as expected", {
  # An example function that passes filter queries in ...
  my_filter <- function(.df, ...) {

    args <- rlang::enquos(...)

    .df |>
      dplyr::filter(!!!args)
  }

  data <- data.frame(
    x = c(1, 2, 3, 4, 5),
    y = c(10, 20, 30, 40, 50)
  )

  # Try with a single criterion in ...
  expect_equal(my_filter(data, x > 2),
               data.frame(x = c(3, 4, 5), y = c(30, 40, 50)))
  expect_equal(my_filter(data, x == 5),
               data.frame(x = 5, y = 50))
  # Try with two criteria in ...
  expect_equal(my_filter(data, x > 2, y < 40),
               data.frame(x = 3, y = 30))
})


test_that("pl_filter_collect() works as expected", {
  skip_on_ci()
  skip_on_cran()
  conn <- get_unit_testing_conn()
  on.exit(DBI::dbDisconnect(conn))

  db_table_name <- "PLFilterCollectTestTable"
  db_country_name <- "PLFilterCollectTestCountry"
  if (db_table_name %in% DBI::dbListTables(conn)) {
    DBI::dbRemoveTable(conn, db_table_name)
  }
  if (db_country_name %in% DBI::dbListTables(conn)) {
    DBI::dbRemoveTable(conn, db_country_name)
  }

  # Create a table to upload
  PLFilterCollectTestTable <- data.frame(Country = as.integer(c(1, 2, 3, 1)),
                                         MyValue = c(3.1415, 2.71828, 42, 5.67e-8))
  PLFilterCollectTestCountry <- data.frame(CountryID = as.integer(c(1, 2, 3)),
                                           Country = c("USA", "ZAF", "GHA"))
  DM <- dm::as_dm(list(PLFilterCollectTestTable = PLFilterCollectTestTable,
                       PLFilterCollectTestCountry = PLFilterCollectTestCountry)) |>
    dm::dm_add_fk(table = PLFilterCollectTestTable,
                  columns = Country,
                  ref_table = PLFilterCollectTestCountry,
                  ref_columns = CountryID)
  dm::copy_dm_to(dest = conn, dm = DM, set_key_constraints = TRUE, temporary = FALSE)
  Sys.sleep(0.5) # Make sure the database has time to put everything in place.

  expect_equal(DBI::dbReadTable(conn, "PLFilterCollectTestTable"), PLFilterCollectTestTable)
  expect_equal(DBI::dbReadTable(conn, "PLFilterCollectTestCountry"), PLFilterCollectTestCountry)

  pl_filter_collect("PLFilterCollectTestTable",
                    # Disable version filtering.
                    version_string = NULL,
                    Country == "USA",
                    conn = conn,
                    collect = TRUE) |>
    expect_equal(tibble::tribble(~Country, ~MyValue,
                                 "USA", 3.1415,
                                 "USA", 5.67e-8))

  # Now try with the schema and fk tables pre-supplied
  fk_tables <- get_all_fk_tables(conn = conn, schema = DM)

  pl_filter_collect("PLFilterCollectTestTable",
                    version_string = NULL,
                    Country == "USA",
                    conn = conn,
                    collect = TRUE,
                    schema = DM,
                    fk_parent_tables = fk_tables) |>
    expect_equal(tibble::tribble(~Country, ~MyValue,
                                 "USA", 3.1415,
                                 "USA", 5.67e-8))

  pl_filter_collect("PLFilterCollectTestTable",
                    version_string = NULL,
                    Country %in% c("USA", "ZAF"),
                    conn = conn,
                    collect = TRUE) |>
    expect_equal(tibble::tribble(~Country, ~MyValue,
                                 "USA", 3.1415,
                                 "ZAF", 2.71828,
                                 "USA", 5.67e-8))

  # Try without collecting
  uncollected <- pl_filter_collect(db_table_name = "PLFilterCollectTestTable",
                                   version_string = NULL,
                                   Country == "USA",
                                   conn = conn,
                                   schema = DM,
                                   fk_parent_tables = fk_tables)
  expect_true(dplyr::is.tbl(uncollected))
  uncollected |>
    dplyr::collect() |>
    expect_equal(tibble::tribble(~Country, ~MyValue,
                                 "USA", 3.1415,
                                 "USA", 5.67e-8))

  # Try without a filter specification. Should get everything back.
  pl_filter_collect("PLFilterCollectTestTable",
                    version_string = NULL,
                    conn = conn,
                    collect = TRUE) |>
    expect_equal(tibble::tribble(~Country, ~MyValue,
                                 "USA", 3.1415,
                                 "ZAF", 2.71828,
                                 "GHA", 42,
                                 "USA", 5.67e-8))

  DBI::dbRemoveTable(conn = conn, name = db_table_name)
  DBI::dbRemoveTable(conn = conn, name = db_country_name)
})


test_that("pl_collect_from_hash() and pl_filter_collect() work with versions", {

  skip_on_ci()
  skip_on_cran()
  conn <- get_unit_testing_conn()
  on.exit(DBI::dbDisconnect(conn))

  # Database table with 2 version columns
  # and 1 column of real data.
  db_table_name <- "PLCollectFromHashVersionsTest"
  if (db_table_name %in% DBI::dbListTables(conn)) {
    DBI::dbRemoveTable(conn, db_table_name)
  }
  db_table <- tibble::tribble(
    ~ValidFromVersion, ~ValidToVersion, ~Country, ~Year, ~val,
    1L, 1L, 1L, 1967L, 1,
    1L, 1L, 2L, 1967L, 2,
    2L, 2L, 1L, 1967L, 3,
    2L, 2L, 2L, 1967L, 4,
    2L, 2L, 1L, 1968L, 5,
    2L, 2L, 2L, 1968L, 6,
    3L, 3L, 3L, 2000L, 7,
    4L, 10L, 3L, 2000L, 8) |>
    as.data.frame()
  db_table_empty <- db_table[0, ]

  # Version table with a VersionID column and a Version column.
  version_table_name <- "VersionTableTest"
  if (version_table_name %in% DBI::dbListTables(conn)) {
    DBI::dbRemoveTable(conn, version_table_name)
  }
  version_table <- data.frame(VersionID = 1:10,
                              Version = paste0("v", 1:10))
  version_table_empty <- version_table[0, ]

  # Country table with a CountryID column
  country_table_name <- "CountryTableTest"
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
    dm::dm_add_fk(table = PLCollectFromHashVersionsTest,
                  columns = ValidFromVersion,
                  ref_table = VersionTableTest,
                  ref_columns = VersionID) |>
    dm::dm_add_fk(table = PLCollectFromHashVersionsTest,
                  columns = ValidToVersion,
                  ref_table = VersionTableTest,
                  ref_columns = VersionID) |>
    dm::dm_add_fk(table = PLCollectFromHashVersionsTest,
                  columns = Country,
                  ref_table = CountryTableTest,
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

  # This is the expected result, all rows.
  # Filter in specific tests below, when needed.
  expected_all <- tibble::tribble(
    ~ValidFromVersion, ~ValidToVersion, ~Country, ~Year, ~val,
    "v1", "v1", "USA", 1967L, 1,
    "v1", "v1", "ZAF", 1967L, 2,
    "v2", "v2", "USA", 1967L, 3,
    "v2", "v2", "ZAF", 1967L, 4,
    "v2", "v2", "USA", 1968L, 5,
    "v2", "v2", "ZAF", 1968L, 6,
    "v3", "v3", "GHA", 2000L, 7,
    "v4", "v10", "GHA", 2000L, 8
  )


  #### Test pl_filter_collect() with versions

  # First attempt is with no filtering on versions.
  # Make sure we download the whole table.
  db_table_name |>
    pl_filter_collect(conn = conn,
                      version_string = NULL,
                      schema = schema,
                      fk_parent_tables = fk_parent_tables,
                      collect = TRUE) |>
    expect_equal(expected_all)

  # Filter to get v1
  db_table_name |>
    pl_filter_collect(version_string = "v1",
                      conn = conn,
                      schema = schema,
                      fk_parent_tables = fk_parent_tables,
                      collect = TRUE) |>
    expect_equal(expected_all |> dplyr::filter(ValidFromVersion == "v1"))

  db_table_name |>
    pl_filter_collect(version_string = "v2",
                      conn = conn,
                      schema = schema,
                      fk_parent_tables = fk_parent_tables,
                      collect = TRUE) |>
    expect_equal(expected_all |> dplyr::filter(ValidFromVersion == "v2"))

  db_table_name |>
    pl_filter_collect(version_string = "v3",
                      conn = conn,
                      schema = schema,
                      fk_parent_tables = fk_parent_tables,
                      collect = TRUE) |>
    expect_equal(expected_all |> dplyr::filter(ValidToVersion == "v3"))

  for (i in 4:10) {
    db_table_name |>
      pl_filter_collect(version_string = paste0("v", i),
                        conn = conn,
                        schema = schema,
                        fk_parent_tables = fk_parent_tables,
                        collect = TRUE) |>
      expect_equal(expected_all |>
                     dplyr::filter(ValidFromVersion == "v4") |>
                     dplyr::mutate(
                       ValidFromVersion = paste0("v", i),
                       ValidToVersion = paste0("v", i)
                     ))
  }

  #### Test pl_filter_collect() with versions and
  #### additional filtering expression
  db_table_name |>
    pl_filter_collect(version_string = "v3",
                      Country == "GHA",
                      conn = conn,
                      schema = schema,
                      fk_parent_tables = fk_parent_tables,
                      collect = TRUE) |>
    expect_equal(expected_all |> dplyr::filter(ValidToVersion == "v3"))
  # Try with different orders for arguments
  db_table_name |>
    pl_filter_collect(Country == "GHA",
                      version_string = "v3",
                      conn = conn,
                      schema = schema,
                      fk_parent_tables = fk_parent_tables,
                      collect = TRUE) |>
    expect_equal(expected_all |> dplyr::filter(ValidToVersion == "v3"))
  pl_filter_collect(Country == "GHA",
                    version_string = "v3",
                    db_table_name = db_table_name,
                    conn = conn,
                    schema = schema,
                    fk_parent_tables = fk_parent_tables,
                    collect = TRUE) |>
    expect_equal(expected_all |> dplyr::filter(ValidToVersion == "v3"))
  pl_filter_collect(version_string = "v3",
                    db_table_name = db_table_name,
                    conn = conn,
                    schema = schema,
                    fk_parent_tables = fk_parent_tables,
                    collect = TRUE,
                    Country == "GHA") |>
    expect_equal(expected_all |> dplyr::filter(ValidToVersion == "v3"))
  pl_filter_collect(version_string = "v3",
                    conn = conn,
                    schema = schema,
                    fk_parent_tables = fk_parent_tables,
                    collect = TRUE,
                    Country == "GHA",
                    db_table_name = db_table_name) |>
    expect_equal(expected_all |> dplyr::filter(ValidToVersion == "v3"))

  # Try a filter in both version_string and ...
  db_table_name |>
    pl_filter_collect(Country == "USA",
                      version_string = "v2",
                      conn = conn,
                      schema = schema,
                      fk_parent_tables = fk_parent_tables,
                      collect = TRUE) |>
    expect_equal(expected_all |> dplyr::filter(ValidToVersion == "v2" &
                                                 Country == "USA"))

  # Try an inequality filter
  db_table_name |>
    pl_filter_collect(version_string = NULL,
                      val <= 6,
                      conn = conn,
                      schema = schema,
                      fk_parent_tables = fk_parent_tables,
                      collect = TRUE) |>
    expect_equal(expected_all |> dplyr::filter(val <= 6))

  # Try an %in% filter
  db_table_name |>
    pl_filter_collect(version_string = NULL,
                      Country %in% c("GHA", "USA"),
                      conn = conn,
                      schema = schema,
                      fk_parent_tables = fk_parent_tables,
                      collect = TRUE) |>
    expect_equal(expected_all |> dplyr::filter(Country %in% c("GHA", "USA")))

  # Add extra conditions
  db_table_name |>
    pl_filter_collect(version_string = NULL,
                      val <= 6 & Year >= 1968,
                      conn = conn,
                      schema = schema,
                      fk_parent_tables = fk_parent_tables,
                      collect = TRUE) |>
    expect_equal(expected_all |> dplyr::filter(val <= 6 & Year >= 1968))
  # Split conditions
  db_table_name |>
    pl_filter_collect(version_string = NULL,
                      val <= 6, Year >= 1968,
                      conn = conn,
                      schema = schema,
                      fk_parent_tables = fk_parent_tables,
                      collect = TRUE) |>
    expect_equal(expected_all |> dplyr::filter(val <= 6 & Year >= 1968))
  db_table_name |>
    pl_filter_collect(version_string = NULL,
                      val <= 6 & Year >= 1968,
                      Country == "ZAF",
                      conn = conn,
                      schema = schema,
                      fk_parent_tables = fk_parent_tables,
                      collect = TRUE) |>
    expect_equal(expected_all |> dplyr::filter(val <= 6 &
                                                 Year >= 1968 &
                                                 Country == "ZAF"))


  #### Test multiple versions
  db_table_name |>
    pl_filter_collect(version_string = c("v1", "v2"),
                      conn = conn,
                      schema = schema,
                      fk_parent_tables = fk_parent_tables,
                      collect = TRUE) |>
    expect_equal(expected_all |> dplyr::filter(ValidToVersion %in% c("v1", "v2")))

  db_table_name |>
    pl_filter_collect(version_string = c("v1", "v2"),
                      Country == "USA",
                      conn = conn,
                      schema = schema,
                      fk_parent_tables = fk_parent_tables,
                      collect = TRUE) |>
    expect_equal(expected_all |> dplyr::filter(ValidToVersion %in% c("v1", "v2"),
                                               Country == "USA"))

  db_table_name |>
    pl_filter_collect(version_string = c("v5", "v2"),
                      Country %in% c("USA", "GHA"),
                      conn = conn,
                      schema = schema,
                      fk_parent_tables = fk_parent_tables,
                      collect = TRUE) |>
    dplyr::arrange(ValidFromVersion) |>
    expect_equal(expected_all |>
                   dplyr::filter(ValidFromVersion %in% c("v4", "v2"),
                                 Country %in% c("GHA", "USA")) |>
                   # The function returns data with the version requested
                   # in the ValidFromVersion and ValidToVersion columns.
                   # Need to do a little fixing to get the
                   # actual expected data frame.
                   dplyr::mutate(
                     ValidFromVersion = dplyr::case_when(
                       ValidFromVersion == "v4" ~ "v5",
                       TRUE ~ ValidFromVersion
                     ),
                     ValidToVersion = dplyr::case_when(
                       ValidToVersion == "v10" ~ "v5",
                       TRUE ~ ValidToVersion
                     )
                   )
    )


  #### Try degenerate cases
  # Setting impossible filters in ... should return an empty data frame.
  db_table_name |>
    pl_filter_collect(version_string = NULL,
                      val == 100,
                      conn = conn,
                      schema = schema,
                      fk_parent_tables = fk_parent_tables,
                      collect = TRUE) |>
    expect_equal(expected_all[0, ])
  db_table_name |>
    pl_filter_collect(val == 100,
                      version_string = "v1",
                      conn = conn,
                      schema = schema,
                      fk_parent_tables = fk_parent_tables,
                      collect = TRUE) |>
    expect_equal(expected_all[0, ])
  # Requesting a non-existent version should emit an error.
  db_table_name |>
    pl_filter_collect(version_string = "bogus",
                      conn = conn,
                      schema = schema,
                      fk_parent_tables = fk_parent_tables,
                      collect = TRUE) |>
    expect_error(regexp = 'Unknown version_string')
  # Setting a zero-length vector for version_string
  # should result in no rows.
  db_table_name |>
    pl_filter_collect(version_string = c(),
                      val == 100,
                      conn = conn,
                      schema = schema,
                      fk_parent_tables = fk_parent_tables,
                      collect = TRUE) |>
    expect_equal(expected_all[0, ])
  # Passing the same version string more than once should give
  # only one copy of the relevant data.
  db_table_name |>
    pl_filter_collect(version_string = c("v3", "v3", "v3"),
                      conn = conn,
                      schema = schema,
                      fk_parent_tables = fk_parent_tables,
                      collect = TRUE) |>
    expect_equal(expected_all |> dplyr::filter(ValidToVersion == "v3"))




  #### Test pl_collect_from_hash() with versions

  # Try to collect from the hashes without specifying a version
  hash_dbt |>
    pl_collect_from_hash(conn = conn,
                         schema = schema,
                         fk_parent_tables = fk_parent_tables,
    ) |>
    expect_equal(expected_all)

  # Collect from hash while specifying a version
  hash_dbt |>
    pl_collect_from_hash(version_string = "v1",
                         conn = conn,
                         schema = schema,
                         fk_parent_tables = fk_parent_tables) |>
    expect_equal(expected_all |> dplyr::filter(ValidFromVersion == "v1"))

  hash_dbt |>
    pl_collect_from_hash(version_string = "v2",
                         conn = conn,
                         schema = schema,
                         fk_parent_tables = fk_parent_tables) |>
    expect_equal(expected_all |> dplyr::filter(ValidFromVersion == "v2"))

  hash_dbt |>
    pl_collect_from_hash(version_string = "v3",
                         conn = conn,
                         schema = schema,
                         fk_parent_tables = fk_parent_tables) |>
    expect_equal(expected_all |> dplyr::filter(ValidToVersion == "v3"))

  for (i in 4:10) {
    hash_dbt |>
      pl_collect_from_hash(version_string = paste0("v", i),
                           conn = conn,
                           schema = schema,
                           fk_parent_tables = fk_parent_tables) |>
      expect_equal(expected_all |> dplyr::filter(ValidFromVersion == "v4"))
  }



  # Clean up
  if (db_table_name %in% DBI::dbListTables(conn)) {
    DBI::dbRemoveTable(conn, db_table_name)
  }
  if (version_table_name %in% DBI::dbListTables(conn)) {
    DBI::dbRemoveTable(conn, version_table_name)
  }
  if (country_table_name %in% DBI::dbListTables(conn)) {
    DBI::dbRemoveTable(conn, country_table_name)
  }
})



test_that("pl_filter_collect() works with compressed remote tables", {

  skip_on_ci()
  skip_on_cran()
  conn <- get_unit_testing_conn()
  on.exit(DBI::dbDisconnect(conn))

  index_map <- create_compression_testing_db(conn)

  # Set the names of the table so we can use the variable in several places
  tname <- "testlocalcompression"

  # Add a few matrices
  # matv1 is the original matrix
  matv1 <- matrix(c(1, 2,
                    3, 4,
                    5, 6),
                  byrow = TRUE,
                  nrow = 3,
                  dimnames = list(c("r1", "r2", "r3"), c("c1", "c2"))) |>
    matsbyname::setrowtype("Product") |> matsbyname::setcoltype("Industry")
  # Create a matsindf data frame for the v1 matrix
  midfv1 <- tibble::tibble(Dataset = "CL-PFU IEA",
                           ValidFromVersion = c("v1.0"),
                           ValidToVersion = c("v1.0"),
                           Country = "USA",
                           EnergyType = "E",
                           Year = 1971,
                           matname = c("Y"),
                           matval = list(matv1))
  # Upsert the original matrix, with compression,
  # but that doesn't matter in this situation.
  rowsv1 <- midfv1 |>
    pl_upsert_and_compress(conn = conn,
                           db_table_name = tname,
                           index_map = index_map,
                           in_place = TRUE,
                           compress = TRUE)

  # matv2 is a modified matrix with the r3, c1 different
  matv2 <- matrix(c(1, 2,
                    3, 4,
                    42, 6),
                  byrow = TRUE,
                  nrow = 3,
                  dimnames = list(c("r1", "r2", "r3"), c("c1", "c2"))) |>
    matsbyname::setrowtype("Product") |> matsbyname::setcoltype("Industry")
  # Create a matsindf data frame for the v2 matrix
  midfv2 <- tibble::tibble(Dataset = "CL-PFU IEA",
                           ValidFromVersion = "v2.0",
                           ValidToVersion = "v2.0",
                           Country = "USA",
                           EnergyType = "E",
                           Year = 1971,
                           matname = "Y",
                           matval = list(matv2))
  # Upsert v2 with compression
  rowsv2 <- midfv2 |>
    pl_upsert_and_compress(conn = conn,
                           db_table_name = tname,
                           index_map = index_map,
                           in_place = TRUE,
                           compress = TRUE)

  # Check that we can get v1 back successfully
  v1_retrieved <- pl_filter_collect(db_table_name = tname,
                                    version_string = "v1.0",
                                    index_map = index_map,
                                    collect = TRUE,
                                    conn = conn,
                                    matrix_class = "matrix")
  expect_equal(v1_retrieved$Y[[1]], matv1)
  # Check that the version columns are correct
  expect_equal(v1_retrieved$ValidFromVersion[[1]], "v1.0" )
  expect_equal(v1_retrieved$ValidToVersion[[1]], "v1.0" )

  # Check that we can get v2 back successfully
  v2_retrieved <- pl_filter_collect(db_table_name = tname,
                                    version_string = "v2.0",
                                    index_map = index_map,
                                    collect = TRUE,
                                    conn = conn,
                                    matrix_class = "matrix")
  expect_equal(v2_retrieved$Y[[1]], matv2)
  # Check that the version columns are correct
  expect_equal(v2_retrieved$ValidFromVersion[[1]], "v2.0")
  expect_equal(v2_retrieved$ValidToVersion[[1]], "v2.0")

  # Test retrieving with the current version.
  # We should get the v2.0 matrix,
  # but the columns should have "current".
  vcurrent_retrieved <- pl_filter_collect(db_table_name = tname,
                                          version_string = "current",
                                          index_map = index_map,
                                          collect = TRUE,
                                          conn = conn,
                                          matrix_class = "matrix")
  expect_equal(vcurrent_retrieved$Y[[1]], matv2)
  # Check that the version columns are correct
  expect_equal(vcurrent_retrieved$ValidFromVersion[[1]], "current")
  expect_equal(vcurrent_retrieved$ValidToVersion[[1]], "current")

  # Test when requesting both v1.0 and v2.0
  v12_retrieved <- pl_filter_collect(db_table_name = tname,
                                     version_string = c("v1.0", "v2.0"),
                                     index_map = index_map,
                                     collect = TRUE,
                                     conn = conn,
                                     matrix_class = "matrix")
  expect_equal(v12_retrieved$Y, list(matv1, matv2))
  expect_equal(v12_retrieved$ValidFromVersion, c("v1.0", "v2.0"))
  # Test reverse order
  v21_retrieved <- pl_filter_collect(db_table_name = tname,
                                     version_string = c("v2.0", "v1.0"),
                                     index_map = index_map,
                                     collect = TRUE,
                                     conn = conn,
                                     matrix_class = "matrix")
  expect_equal(v21_retrieved$Y, list(matv2, matv1))
  expect_equal(v21_retrieved$ValidFromVersion, c("v2.0", "v1.0"))





  # Test uploading an updated instance of one of the matrices
  # with a 0 in it in the outdated version (v1).
  # This should fail, because we're trying to update
  # an old version of the database.
  matv1b <- matv1
  matv1b[2, 1] <- 0
  midfv1b <- midfv1 |>
    dplyr::mutate(
      matval = list(matv1b)
    )
  # Upsert v1b with compression
  midfv1b |>
    pl_upsert_and_compress(conn = conn,
                           db_table_name = tname,
                           index_map = index_map,
                           in_place = TRUE,
                           compress = TRUE) |>
    expect_error("local_df contains older versions than remote_df in")
  v1b_retrieved <- pl_filter_collect(db_table_name = tname,
                                     version_string = "v1.0",
                                     index_map = index_map,
                                     collect = TRUE,
                                     conn = conn,
                                     matrix_class = "matrix")
  # Because we couldn't update, we should get v1
  expect_equal(v1b_retrieved$Y[[1]], matv1)


  # Can we still filter on the version string columns after the fact,
  # if we do not create matsindf?


  # Test retrieving without specifying a version string
  # Do we get the right answer?


  # Test uploading with a changed value
  # for the newest version.
  matv2b <- matv2
  matv2b[1, 2] <- 3.1415926
  midfv2b <- midfv2 |>
    dplyr::mutate(
      matval = list(matv2b)
    )
  # Upsert v2b with compression
  rowsv2b <- midfv2b |>
    pl_upsert_and_compress(conn = conn,
                           db_table_name = tname,
                           index_map = index_map,
                           in_place = TRUE,
                           compress = TRUE)
  v2b_retrieved <- pl_filter_collect(db_table_name = tname,
                                     version_string = "v2.0",
                                     index_map = index_map,
                                     collect = TRUE,
                                     conn = conn,
                                     matrix_class = "matrix")
  expect_equal(v2b_retrieved$Y[[1]], matv2b)




  # Test uploading an updated version of one of the matrices
  # with a 0 in it.
  # This is a tough test, because we should NOT get the number 3
  # in the 2nd row, 1st column.
  # We should get the 0.
  matv2c <- matv2b
  matv2c[2, 1] <- 0
  midfv2c <- midfv2b |>
    dplyr::mutate(
      matval = list(matv2c)
    )
  # Upsert v2b with compression
  rowsv2c <- midfv2c |>
    pl_upsert_and_compress(conn = conn,
                           db_table_name = tname,
                           index_map = index_map,
                           in_place = TRUE,
                           compress = TRUE)
  v2c_retrieved <- pl_filter_collect(db_table_name = tname,
                                     version_string = "v2.0",
                                     index_map = index_map,
                                     collect = TRUE,
                                     conn = conn,
                                     matrix_class = "matrix")
  # Verify that we get the 0
  expect_equal(v2c_retrieved$Y[[1]][2, 1], 0)
  expect_equal(v2c_retrieved$Y[[1]], matv2c)

  clean_compression_testing_db(conn)
})


