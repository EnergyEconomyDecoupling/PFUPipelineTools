test_that("release_target() works as expected", {

  df <- data.frame(names = c("A", "B"), values = c(1, 2))
  # Create a temp folder and pinboard
  tdir <- tempdir()
  pinboard <- pins::board_folder(tdir)

  # Check not releeasing
  no_release <- release_target(tdir, targ = df, pin_name = "df", release = FALSE)
  expect_equal(no_release, "Release not requested.")

  # Check doing a release
  yes_release <- release_target(tdir, targ = df, pin_name = "df", release = TRUE) |>
    suppressMessages()
  expect_equal(yes_release, "df")
  expect_true("df" %in% list.files(path = tdir))
  recursed <- list.files(path = tdir, recursive = TRUE)
  expect_true(startsWith(recursed[[1]], "df"))
  expect_true(endsWith(recursed[[1]], "data.txt"))
  expect_true(startsWith(recursed[[2]], "df"))
  expect_true(endsWith(recursed[[2]], "df.rds"))
  # Delete the temporary folder
  unlink(tdir, recursive = TRUE, force = TRUE)
})


test_that("pl_hash() works as expected with in-memory data frame", {
  # Example data frame
  DF <- tibble::tribble(~Country, ~Year, ~Value,
                        "USA", 1967, 42,
                        "ZAF", 1967, 43)
  the_hash <- DF |>
    pl_hash(table_name = "MyTable")

  expect_equal(names(the_hash), c(PFUPipelineTools::hashed_table_colnames$db_table_name,
                                  "Country",
                                  "Year",
                                  PFUPipelineTools::hashed_table_colnames$nested_hash_colname))
  expect_equal(nrow(the_hash), 2)
  expect_equal(the_hash[[PFUPipelineTools::hashed_table_colnames$db_table_name]] |>
                 unique(),
               "MyTable")

  # Now try with non-NULL hash_group_cols
  the_hash2 <- DF |>
    pl_hash(table_name = "MyTable",
            usual_hash_group_cols = NULL,
            additional_hash_group_cols = c("Country", "Year"))
  # Both Country and Year will be preserved
  expect_equal(nrow(the_hash2), 2)
  expect_equal(the_hash2[[PFUPipelineTools::hashed_table_colnames$db_table_name]] |>
                 unique(),
               "MyTable")

  # Try with too many grouping variables
  the_hash3 <- DF |>
    pl_hash(table_name = "MyTable",
            additional_hash_group_cols = PFUPipelineTools::usual_hash_group_cols)
  # Should also preserve Country and Year
  expect_equal(nrow(the_hash3), 2)
  expect_equal(the_hash3[[PFUPipelineTools::hashed_table_colnames$db_table_name]] |>
                 unique(),
               "MyTable")
  expect_equal(the_hash3[[PFUPipelineTools::hashed_table_colnames$nested_hash_colname]][[1]],
               "dfc3511d250aee5b3ac45c08cba8c2a3")
  expect_equal(the_hash3[[PFUPipelineTools::hashed_table_colnames$nested_hash_colname]][[2]],
               "d25bbeb674b4041524d283ff08fadc0c")
})


test_that("pl_hash() works with remote table", {
  skip_on_ci()
  skip_on_cran()
  conn <- DBI::dbConnect(drv = RPostgres::Postgres(),
                         dbname = "unit_testing",
                         host = "mexer.site",
                         port = 5432,
                         user = "mkh2")
  on.exit(DBI::dbDisconnect(conn))

  # Create a test table that has same Country
  # for all rows.
  # Thus, Country would normally be nested.
  df <- data.frame(Country = as.integer(c(1, 1, 1, 1)),
                   Year = as.integer(c(1971, 1972, 1973, 1974)),
                   EnergyType = as.integer(c(1, 1, 2, 2)),
                   Value = c(1/3, 43, 44, 45))
  pl_hash_df <- pl_hash(df,
                        table_name = "TestPLHash",
                        additional_hash_group_cols = c("Country", "EnergyType"),
                        usual_hash_group_cols = NULL)
  expect_equal(nrow(pl_hash_df), 2)
  expected_colnames <- c("DBTableName", "Country", "EnergyType", "NestedDataHash")
  expect_equal(colnames(pl_hash_df), expected_colnames)


  # Upload to database
  DBI::dbWriteTable(conn, "TestPLHash", df, overwrite = TRUE)

  pl_hash_tbl <- pl_hash(table_name = "TestPLHash",
                         conn = conn,
                         additional_hash_group_cols = c("EnergyType"),
                         usual_hash_group_cols = NULL)
  expect_equal(nrow(pl_hash_tbl), 2)
  expect_equal(colnames(pl_hash_tbl), expected_colnames)



  DBI::dbRemoveTable(conn, "TestPLHash")
})


test_that("pl_upsert_and_compress() works for zero matrices", {
  skip_on_ci()
  skip_on_cran()
  conn <- get_unit_testing_conn()
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
  index_map <- list(Product = data.frame(IndexID = as.integer(1:3),
                                     Index = c("r1", "r2", "r3")),
                    Industry = data.frame(IndexID = as.integer(1:2),
                                     Index = c("c1", "c2")))

  # Create a zero matrix
  zerom <- matrix(c(0, 0,
                    0, 0,
                    0, 0), nrow = 3, dimnames = list(c("r1", "r2", "r3"), c("c1", "c2"))) |>
    matsbyname::setrowtype("Product") |> matsbyname::setcoltype("Industry")
  # Create a matsindf data frame
  midf <- tibble::tibble(matname = c("zerom1", "zerom2"),
                         matval = list(zerom, zerom))
  no_rows <- midf |>
    pl_upsert_and_compress(conn = conn,
                           db_table_name = "testzeromatrix",
                           index_map = index_map,
                           compress = FALSE)
  # Check that there are no rows in the hashed table
  expect_equal(nrow(no_rows), 0)
  # Check that there are no rows in the table
  should_be_no_rows <- DBI::dbReadTable(conn, name = "testzeromatrix")
  expect_equal(nrow(should_be_no_rows), 0)

  # Now upsert zerom while preserving rows
  twelve_rows <- midf |>
    pl_upsert_and_compress(conn = conn,
                           db_table_name = "testzeromatrix",
                           index_map = index_map,
                           in_place = TRUE,
                           retain_zero_structure = TRUE,
                           compress = FALSE)
  # The hash should come back with 1 row
  expect_equal(nrow(twelve_rows), 1)
  # Check that there are twelve rows in the table
  should_be_twelve_rows <- DBI::dbReadTable(conn, name = "testzeromatrix")
  expect_equal(nrow(should_be_twelve_rows), 12)

  # Now try to use pl_filter_collect() to get the data.
  rctypes <- tibble::tribble(~matname, ~rowtype, ~coltype,
                             "zerom1", "Product", "Industry",
                             "zerom2", "Product", "Industry")



  # The following should give zero matrices with
  # row and column names
  filter_collected <- pl_filter_collect(db_table_name = "testzeromatrix",
                                        version_string = NULL,
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


test_that("pl_upsert() works with local table compression", {
  # pl_upsert() is deprecated.
  # We will need to remove this test when
  # pl_upsert() is removed.
  conn <- get_unit_testing_conn()
  on.exit(DBI::dbDisconnect(conn))

  # Set the name of the table so we can use the variable in several places
  tname <- "testlocalcompression"

  # Start with a fresh slate
  if (DBI::dbExistsTable(conn = conn, name = tname)) {
    DBI::dbRemoveTable(conn = conn, name = tname)
  }

  # Create data model
  dm <- list(testlocalcompression = data.frame(ValidFromVersion = as.integer(1),
                                               ValidToVersion = as.integer(2),
                                               matname = "pimat",
                                               i = as.integer(1),
                                               j = as.integer(1),
                                               value = 3.1415926) |>
               # Delete all rows, but keep names and column types
               dplyr::filter(FALSE)) |>
    dm::new_dm() |>
    dm::dm_add_pk(testlocalcompression, columns = c(ValidFromVersion, ValidToVersion,
                                                    matname, i, j))
  dm::copy_dm_to(conn, dm = dm, temporary = FALSE)
  # Create index map
  index_map <- list(Product = data.frame(IndexID = as.integer(1:3),
                                         Index = c("r1", "r2", "r3")),
                    Industry = data.frame(IndexID = as.integer(1:2),
                                          Index = c("c1", "c2")))
  # Create a couple matrices
  # matv1 is the original matrix
  matv1 <- matrix(c(1, 2,
                    3, 4,
                    5, 6),
                  byrow = TRUE,
                  nrow = 3,
                  dimnames = list(c("r1", "r2", "r3"), c("c1", "c2"))) |>
    matsbyname::setrowtype("Product") |> matsbyname::setcoltype("Industry")
  # matv2 is a modified matrix with the r3, c1 different
  matv2 <- matrix(c(1, 2,
                    3, 4,
                    42, 6),
                  byrow = TRUE,
                  nrow = 3,
                  dimnames = list(c("r1", "r2", "r3"), c("c1", "c2"))) |>
    matsbyname::setrowtype("Product") |> matsbyname::setcoltype("Industry")
  # Create a matsindf data frame for the v1 matrix
  midfv1 <- tibble::tibble(ValidFromVersion = 1,
                           ValidToVersion = 1,
                           matname = c("mat"),
                           matval = list(matv1))
  # Upsert the original matrix, without compression.
  rowsv1 <- midfv1 |>
    pl_upsert(conn = conn,
              db_table_name = tname,
              index_map = index_map,
              in_place = TRUE,
              compress = FALSE)
  # Check that there are 6 rows in remote table
  should_be_six_rows <- DBI::dbReadTable(conn, name = tname)
  expect_equal(nrow(should_be_six_rows), 6)

  # Create a matsindf data frame for the v2 matrix
  midfv2 <- tibble::tibble(ValidFromVersion = 2,
                           ValidToVersion = 2,
                           matname = c("mat"),
                           matval = list(matv2))

  # Upsert v2 without compression
  rowsv2 <- midfv2 |>
    pl_upsert(conn = conn,
              db_table_name = tname,
              index_map = index_map,
              in_place = TRUE,
              compress = FALSE)
  # Check that there are 12 rows in remote table
  should_be_twelve_rows <- DBI::dbReadTable(conn, name = tname)
  expect_equal(nrow(should_be_twelve_rows), 12)

  # Remove the rows with v2
  conn |>
    DBI::dbExecute('DELETE FROM testlocalcompression WHERE "ValidFromVersion" = 2 AND "ValidToVersion" = 2;')

  # Now upsert with compression
  # compress = TRUE is the default
  rowsv2 <- midfv2 |>
    pl_upsert(conn = conn,
              db_table_name = tname,
              index_map = index_map,
              in_place = TRUE,
              compress = TRUE)

  # Check that we have 7 rows
  should_be_seven_rows <- DBI::dbReadTable(conn, name = tname)
  expect_equal(nrow(should_be_seven_rows), 7)
  # Check that the original rows are present
  resv1 <- dplyr::tbl(conn, "testlocalcompression") |>
    dplyr::filter(ValidFromVersion == 1) |>
    dplyr::collect() |>
    dplyr::arrange(i, j)
  expected_resv1 <- tibble::tibble(ValidFromVersion = 1,
                                   ValidToVersion = c(2, 2, 2, 2, 1, 2),
                                   matname = "mat",
                                   i = c(1, 1, 2, 2, 3, 3),
                                   j = c(1, 2, 1, 2, 1, 2),
                                   value = 1:6)
  testthat::expect_equal(resv1, expected_resv1)
  resv2 <- dplyr::tbl(conn, "testlocalcompression") |>
    dplyr::filter(ValidToVersion == 2) |>
    dplyr::collect() |>
    dplyr::arrange(i, j)
  expected_resv2 <- tibble::tibble(ValidFromVersion = c(1, 1, 1, 1, 2, 1),
                                   ValidToVersion = 2,
                                   matname = "mat",
                                   i = c(1, 1, 2, 2, 3, 3),
                                   j = c(1, 2, 1, 2, 1, 2),
                                   value = c(1, 2, 3, 4, 42, 6))
  testthat::expect_equal(resv2, expected_resv2)

  # Clean up after ourselves
  DBI::dbRemoveTable(conn = conn, name = "testlocalcompression")
})


test_that("pl_upsert_and_compress() works with local table compression", {
  conn <- get_unit_testing_conn()
  on.exit(DBI::dbDisconnect(conn))
  index_map <- create_compression_testing_db(conn)

  # Set the names of the table so we can use the variable in several places
  tname <- "testlocalcompression"

  # Create a couple matrices
  # matv1 is the original matrix
  matv1 <- matrix(c(1, 2,
                    3, 4,
                    5, 6),
                  byrow = TRUE,
                  nrow = 3,
                  dimnames = list(c("r1", "r2", "r3"), c("c1", "c2"))) |>
    matsbyname::setrowtype("Product") |> matsbyname::setcoltype("Industry")
  # matv2 is a modified matrix with the r3, c1 different
  matv2 <- matrix(c(1, 2,
                    3, 4,
                    42, 6),
                  byrow = TRUE,
                  nrow = 3,
                  dimnames = list(c("r1", "r2", "r3"), c("c1", "c2"))) |>
    matsbyname::setrowtype("Product") |> matsbyname::setcoltype("Industry")
  # Create a matsindf data frame for the v1 matrix
  midfv1 <- tibble::tibble(Dataset = "CL-PFU IEA",
                           ValidFromVersion = c("v1.0"),
                           ValidToVersion = c("current"),
                           Country = "USA",
                           EnergyType = "E",
                           Year = 1971,
                           matname = c("Y"),
                           matval = list(matv1))
  # Upsert the original matrix, without compression.
  rowsv1 <- midfv1 |>
    pl_upsert_and_compress(conn = conn,
                           db_table_name = tname,
                           index_map = index_map,
                           in_place = TRUE,
                           compress = FALSE)
  # Check that there are 6 rows in remote table
  should_be_six_rows <- DBI::dbReadTable(conn, name = tname)
  expect_equal(nrow(should_be_six_rows), 6)

  # Create a matsindf data frame for the v2 matrix
  midfv2 <- tibble::tibble(Dataset = "CL-PFU IEA",
                           ValidFromVersion = "v2.0",
                           ValidToVersion = "v2.0",
                           Country = "GHA",
                           EnergyType = "E",
                           Year = 1972,
                           matname = "V",
                           matval = list(matv2))

  # Upsert v2 without compression
  rowsv2 <- midfv2 |>
    pl_upsert_and_compress(conn = conn,
                           db_table_name = tname,
                           index_map = index_map,
                           in_place = TRUE,
                           compress = FALSE)
  # Check that there are 12 rows in remote table
  should_be_twelve_rows <- DBI::dbReadTable(conn, name = tname)
  expect_equal(nrow(should_be_twelve_rows), 12)

  # Remove rows with v2
  conn |>
    DBI::dbExecute('DELETE FROM testlocalcompression WHERE "ValidFromVersion" = 2 AND "ValidToVersion" = 2;')

  # Now upsert with compression
  # compress = TRUE is the default
  rowsv2 <- midfv2 |>
    pl_upsert_and_compress(conn = conn,
                           db_table_name = tname,
                           index_map = index_map,
                           in_place = TRUE)

  # We added data with a new country.
  # Check that we have 12 rows now.
  should_be_twelve_rows2 <- DBI::dbReadTable(conn, name = tname)
  expect_equal(nrow(should_be_twelve_rows2), 12)
  # Check that the original rows are present
  resv1 <- dplyr::tbl(conn, tname) |>
    dplyr::filter(ValidFromVersion == 1) |>
    dplyr::collect() |>
    dplyr::arrange(i, j)
  expected_resv1 <- tibble::tibble(Dataset = 5,
                                   ValidFromVersion = 1,
                                   ValidToVersion = version_info$current_version_int,
                                   Country = 146,
                                   EnergyType = 1,
                                   Year = 1971,
                                   matname = 8,
                                   i = c(1, 1, 2, 2, 3, 3),
                                   j = c(5, 6, 5, 6, 5, 6),
                                   value = 1:6)
  testthat::expect_equal(resv1, expected_resv1)
  resv2 <- dplyr::tbl(conn, tname) |>
    dplyr::filter(ValidFromVersion == 2,
                  ValidToVersion == version_info$current_version_int) |>
    dplyr::collect() |>
    dplyr::arrange(i, j)
  expected_resv2 <- tibble::tibble(Dataset = 5,
                                   ValidFromVersion = 2,
                                   ValidToVersion = version_info$current_version_int,
                                   Country = 49,
                                   EnergyType = 1,
                                   Year = 1972,
                                   matname = 7,
                                   i = c(1, 1, 2, 2, 3, 3),
                                   j = c(5, 6, 5, 6, 5, 6),
                                   value = c(1, 2, 3, 4, 42, 6))
  testthat::expect_equal(resv2, expected_resv2)

  # Remove rows with v2
  conn |>
    DBI::dbExecute('DELETE FROM testlocalcompression WHERE "ValidFromVersion" = 2;')

  # Add a matrix with a modified value in the same version
  # but all other metadata same.
  # This should update the value in the table but
  # leave everything else unchanged.
  midfv3 <- tibble::tibble(Dataset = "CL-PFU IEA",
                           ValidFromVersion = c("v1.0"),
                           ValidToVersion = c("v1.0"),
                           Country = "USA",
                           EnergyType = "E",
                           Year = 1971,
                           matname = c("Y"),
                           matval = list(matv2))
  rowsv3 <- midfv3 |>
    pl_upsert_and_compress(conn = conn,
                           db_table_name = tname,
                           index_map = index_map,
                           in_place = TRUE)
  resv3 <- dplyr::tbl(conn, tname) |>
    dplyr::filter(ValidFromVersion == 1,
                  ValidToVersion == version_info$current_version_int) |>
    dplyr::collect() |>
    dplyr::arrange(i, j)
  expected_resv3 <- tibble::tibble(Dataset = 5,
                                   ValidFromVersion = 1,
                                   ValidToVersion = version_info$current_version_int,
                                   Country = 146,
                                   EnergyType = 1,
                                   Year = 1971,
                                   matname = 8,
                                   i = c(1, 1, 2, 2, 3, 3),
                                   j = c(5, 6, 5, 6, 5, 6),
                                   value = c(1, 2, 3, 4, 42, 6))
  expect_equal(resv3, expected_resv3)

  # Put v1 back the way it was
  foo <- midfv1 |>
    dplyr::mutate(ValidToVersion = "v1.0") |>
    pl_upsert_and_compress(conn = conn,
                           db_table_name = tname,
                           index_map = index_map,
                           in_place = TRUE)

  # Now add an updated matrix with a new version.
  # So same as the previous test but with new version.
  midfv4 <- tibble::tibble(Dataset = "CL-PFU IEA",
                           ValidFromVersion = c("v2.0"),
                           ValidToVersion = c("v2.0"),
                           Country = "USA",
                           EnergyType = "E",
                           Year = 1971,
                           matname = c("Y"),
                           matval = list(matv2))
  rowsv4 <- midfv4 |>
    pl_upsert_and_compress(conn = conn,
                           db_table_name = tname,
                           index_map = index_map,
                           in_place = TRUE)
  resv4 <- dplyr::tbl(conn, tname) |>
    dplyr::collect() |>
    dplyr::arrange(value)
  expected_resv4 <- tibble::tibble(Dataset = 5,
                                   ValidFromVersion = c(1, 1, 1, 1, 1, 1, 2),
                                   ValidToVersion = c(version_info$current_version_int,
                                                      version_info$current_version_int,
                                                      version_info$current_version_int,
                                                      version_info$current_version_int,
                                                      1,
                                                      version_info$current_version_int,
                                                      version_info$current_version_int),
                                   Country = 146,
                                   EnergyType = 1,
                                   Year = 1971,
                                   matname = 8,
                                   i = c(1, 1, 2, 2, 3, 3, 3),
                                   j = c(5, 6, 5, 6, 5, 6, 5),
                                   value = c(1, 2, 3, 4, 5, 6, 42)) |>
    dplyr::arrange(value)
  expect_equal(resv4, expected_resv4)

  # Put things back the way they were
  DBI::dbExecute(conn, "DELETE FROM testlocalcompression")
  foo <- midfv1 |>
    pl_upsert_and_compress(conn = conn,
                           db_table_name = tname,
                           index_map = index_map,
                           in_place = TRUE)
  # Create a Y matrix with a slightly different value
  # to test pl_upsert_and_compress() when we are within tol.
  matv3 <- matv1
  matv3[3, 2] = matv3[3,2] - 1.0e-7
  expect_equal(matv3[3, 2] - 6, -1e-7)
  midfv5 <- tibble::tibble(Dataset = "CL-PFU IEA",
                           ValidFromVersion = c("v2.0"),
                           ValidToVersion = c("v2.0"),
                           Country = "USA",
                           EnergyType = "E",
                           Year = 1971,
                           matname = c("Y"),
                           matval = list(matv3))
  rowsv5 <- midfv5 |>
    pl_upsert_and_compress(conn = conn,
                           db_table_name = tname,
                           index_map = index_map,
                           in_place = TRUE)
  resv5 <- dplyr::tbl(conn, tname) |>
    dplyr::collect() |>
    dplyr::arrange(value)
  # No rows should have been added.
  expect_equal(nrow(resv5), 6)
  # Nothing should have changed, because the value of
  # matv3[3, 2] is within tol (1e-6) of the original.
  resv5 |>
    dplyr::filter(i == 3, j == 6) |>
    dplyr::pull(value) |>
    magrittr::subtract(6) |>
    expect_equal(0)

  # Now try with a smaller value for tol
  # This should make changes.
  rowsv6 <- midfv5 |>
    pl_upsert_and_compress(conn = conn,
                           db_table_name = tname,
                           index_map = index_map,
                           # Smaller than 1e-10, so the modification of 1e-10
                           # should show up as a modification to the
                           # database.
                           tol = 1.0e-8,
                           in_place = TRUE)
  resv6 <- dplyr::tbl(conn, tname) |>
    dplyr::collect() |>
    dplyr::arrange(value)
  resv6 |>
    dplyr::filter(ValidFromVersion == 2, i == 3, j == 6) |>
    dplyr::pull(value) |>
    magrittr::subtract(6) |>
    expect_equal(-1.0e-7)

  # Clean up after ourselves
  clean_compression_testing_db(conn)
})















test_that("pl_upsert_and_compress() works with more metadata columns and new rows/cols", {
  conn <- get_unit_testing_conn()
  on.exit(DBI::dbDisconnect(conn))
  index_map <- create_compression_testing_db(conn)

  # Set the names of the table so we can use the variable in several places
  tname <- "testlocalcompression"

  # Create a couple matrices
  # matv1 is the original matrix
  matv1 <- matrix(c(1, 2,
                    3, 4,
                    5, 6),
                  byrow = TRUE,
                  nrow = 3,
                  dimnames = list(c("r1", "r2", "r3"), c("c1", "c2"))) |>
    matsbyname::setrowtype("Product") |> matsbyname::setcoltype("Industry")
  # matv2 is a modified matrix with the r3, c1 value different and
  # adding an additional row and column.
  # matv2 has new data (and new row, col names) compared to matv1.
  matv2 <- matrix(c(1, 2, 0,
                    3, 4, 0,
                    31, 6, 0,
                    41, 42, 43),
                  byrow = TRUE,
                  nrow = 4,
                  dimnames = list(c("r1", "r2", "r3", "r4"),
                                  c("c1", "c2", "c3"))) |>
    matsbyname::setrowtype("Product") |> matsbyname::setcoltype("Industry")
  # Create a matsindf data frame for the v1 matrix
  midfv1 <- tibble::tibble(Dataset = "CL-PFU IEA",
                           ValidFromVersion = c("v1.0"),
                           ValidToVersion = c("v1.0"),
                           Country = "USA",
                           EnergyType = "X",
                           Year = 1971,
                           matname = c("V"),
                           matval = list(matv1))

  # Upsert the original matrix, without compression.
  rowsv1 <- midfv1 |>
    pl_upsert_and_compress(conn = conn,
                           db_table_name = tname,
                           index_map = index_map,
                           in_place = TRUE)
  # Verify that we have 6 rows, one for each entry in the matrix
  should_be_six_rows <- DBI::dbReadTable(conn, name = tname)
  expect_equal(nrow(should_be_six_rows), 6)

  # Try adding a new matrix with more rows/cols
  midfv2 <- midfv1 |>
    dplyr::bind_rows(
      tibble::tibble(Dataset = "CL-PFU IEA",
                     ValidFromVersion = c("v1.0"),
                     ValidToVersion = c("v1.0"),
                     Country = "USA",
                     EnergyType = "X",
                     Year = 1971,
                     matname = c("V"),
                     matval = list(matv2))
      )
  rowsv2 <- midfv2 |>
    pl_upsert_and_compress(conn = conn,
                           db_table_name = tname,
                           index_map = index_map,
                           in_place = TRUE)
  should_be_nine_rows <- DBI::dbReadTable(conn, name = tname)
  expect_equal(nrow(should_be_nine_rows), 9)
  # Verify that the new data are in place
  should_be_nine_rows |>
    dplyr::filter(i == 3, j == 5) |>
    dplyr::pull(value) |>
    expect_equal(31)
  should_be_nine_rows |>
    dplyr::filter(i == 4, j == 5) |>
    dplyr::pull(value) |>
    expect_equal(41)
  should_be_nine_rows |>
    dplyr::filter(i == 4, j == 6) |>
    dplyr::pull(value) |>
    expect_equal(42)
  should_be_nine_rows |>
    dplyr::filter(i == 4, j == 7) |>
    dplyr::pull(value) |>
    expect_equal(43)

  # Add another matrix with decimals
  matv3 <- matv2
  matv3[2, 3] <- 3.1415926
  midfv3 <- midfv2 |>
    dplyr::bind_rows(
      tibble::tibble(Dataset = "CL-PFU IEA",
                     ValidFromVersion = c("v1.0"),
                     ValidToVersion = c("v1.0"),
                     Country = "USA",
                     EnergyType = "X",
                     Year = 1971,
                     matname = c("V"),
                     matval = list(matv3))
    )

  rowsv3 <- midfv3 |>
    pl_upsert_and_compress(conn = conn,
                           db_table_name = tname,
                           index_map = index_map,
                           in_place = TRUE)
  should_be_ten_rows <- DBI::dbReadTable(conn, name = tname)
  expect_equal(nrow(should_be_ten_rows), 10)
  # Verify that the new data are in place
  should_be_ten_rows |>
    dplyr::filter(i == 2, j == 7) |>
    dplyr::pull(value) |>
    expect_equal(3.1415926)

  # Add a smaller matrix but with the same version number.
  # This should delete some rows.
  matv4 <- matv3[-c(1, 2, 4), -1, drop = FALSE] |>
    matsbyname::setrowtype("Product") |> matsbyname::setcoltype("Industry")
  midfv4 <- tibble::tibble(Dataset = "CL-PFU IEA",
                           ValidFromVersion = c("v1.0"),
                           ValidToVersion = c("v1.0"),
                           Country = "USA",
                           EnergyType = "X",
                           Year = 1971,
                           matname = c("V"),
                           matval = list(matv4))

  rowsv4 <- midfv4 |>
    pl_upsert_and_compress(conn = conn,
                           db_table_name = tname,
                           index_map = index_map,
                           in_place = TRUE)

  should_be_one_row <- DBI::dbReadTable(conn, name = tname)
  expect_equal(nrow(should_be_one_row), 1)
  expected <- tibble::tibble(Dataset = 5,
                             ValidFromVersion = 1,
                             ValidToVersion = version_info$current_version_int,
                             Country = 146,
                             EnergyType = 2,
                             Year = 1971,
                             matname = 7,
                             i = 3,
                             j = 6,
                             value = 6) |>
    as.data.frame()
  expect_equal(should_be_one_row, expected)

  # Clean up after ourselves
  clean_compression_testing_db(conn)
})
