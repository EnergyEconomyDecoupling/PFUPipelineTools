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
                                  "Year",
                                  PFUPipelineTools::hashed_table_colnames$nested_hash_colname))
  expect_equal(nrow(the_hash), 1)
  expect_equal(the_hash[[PFUPipelineTools::hashed_table_colnames$db_table_name]],
               "MyTable")

  # Now try with non-NULL hash_group_cols
  the_hash2 <- DF |>
    pl_hash(table_name = "MyTable",
            additional_hash_group_cols = c("Country", "Year"))
  # Both Country and Year will be preserved
  expect_equal(nrow(the_hash2), 2)
  expect_equal(the_hash2[[PFUPipelineTools::hashed_table_colnames$db_table_name]] |>
                 unique(),
               "MyTable")

  # Try with too many grouping variables
  the_hash3 <- DF |>
    pl_hash(table_name = "MyTable",
            additional_hash_group_cols = PFUPipelineTools::additional_hash_group_cols)
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
                         host = "eviz.cs.calvin.edu",
                         port = 5432,
                         user = "postgres")
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
                        additional_hash_group_cols = c("Country", "EnergyType"))
  expect_equal(nrow(pl_hash_df), 2)
  expected_colnames <- c("DBTableName", "Country", "EnergyType", "NestedDataHash")
  expect_equal(colnames(pl_hash_df), expected_colnames)


  # Upload to database
  DBI::dbWriteTable(conn, "TestPLHash", df, overwrite = TRUE)

  pl_hash_tbl <- pl_hash(table_name = "TestPLHash",
                         conn = conn,
                         additional_hash_group_cols = c("EnergyType"))
  expect_equal(nrow(pl_hash_tbl), 2)
  expect_equal(colnames(pl_hash_tbl), expected_colnames)



  DBI::dbRemoveTable(conn, "TestPLHash")
})

