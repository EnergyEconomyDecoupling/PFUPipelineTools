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


test_that("tar_ungroup() works as expected", {
  df <- data.frame(A = c("Smith", "Smith", "Jones"),
                   B = c(1, 2, 3),
                   C = c(4, 5, 6)) |>
    dplyr::group_by(A)
  expect_equal(dplyr::group_vars(df), "A")
  grouped <- targets::tar_group(df)
  expect_equal(dplyr::group_vars(grouped), character())
  expect_equal(grouped$tar_group, c(2, 2, 1))
  res <- df |>
    tar_ungroup()
  expect_equal(dplyr::groups(res), list())
})


test_that("read_pin_version() works as expected", {

  testthat::skip_on_ci()

  phi_vecs <- read_pin_version(pin_name = "phi_vecs", database_version = 1.2)
  expect_equal(names(phi_vecs), c("Country", "Year", "phi"))

  phi_vecs_string <- read_pin_version(pin_name = "phi_vecs", database_version = "v1.2")
  expect_equal(names(phi_vecs), c("Country", "Year", "phi"))
})


test_that("read_pin_version() fails when a non-existent pin is supplied", {
  testthat::skip_on_ci()

  expect_error(read_pin_version(pin_name = "bogus_pin", database_version = 1.2),
               "CL-PFU database product bogus_pin does not exist")
})
