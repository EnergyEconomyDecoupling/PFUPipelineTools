test_that("filter_countries_years() works as expected", {
  res <- IEATools::sample_iea_data_path() |>
    IEATools::load_tidy_iea_df() |>
    filter_countries_years(countries = c("ZAF"), years = 1960:1999)
  expect_equal(res$Country |> unique(), "ZAF")
  expect_equal(res$Year |> unique(), 1971)
})
