test_that("filter_countries_years() works as expected", {
  iea_data <- IEATools::sample_iea_data_path() |>
    IEATools::load_tidy_iea_df()

  res <- iea_data |>
    filter_countries_years(countries = c("ZAF"), years = 1960:1999)
  expect_equal(res$Country |> unique(), "ZAF")
  expect_equal(res$Year |> unique(), 1971)

  # Try when countries = "all" and years = "all"
  res_2 <- iea_data |>
    filter_countries_years(countries = "all", years = "all")
  expect_equal(res_2, iea_data)

  # Try when have only 1 country but you ask for all countries
  res_3 <- iea_data |>
    dplyr::filter(Country == "ZAF") |>
    filter_countries_years(countries = "all", years = 1971)
  expect_equal(res_3, iea_data |> dplyr::filter(Country == "ZAF", Year == 1971))

  # Try when years = "all" and country needs to be filtered
  res_4 <- iea_data |>
    dplyr::filter(Year == 2000) |>
    filter_countries_years(countries = "ZAF", years = "all")
  expect_equal(res_4, iea_data |> dplyr::filter(Country == "ZAF", Year == 2000))
})


