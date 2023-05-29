#' Extract specific country and year data
#'
#' Data is extracted according to the `countries` and `years` objects
#' in a way that is amenable to drake subtargets.
#' `dplyr::filter()` does the subsetting.
#'
#' @param .df A data frame containing cleaned data with lots of countries and years.
#' @param countries A list of country codes for countries to be analyzed.
#'                  "all" means return all countries.
#' @param years A vector of years. "all" means return all years.
#' @param country,year See `IEATools::iea_cols`.
#'
#' @return A data frame with the desired IEA data only.
#'
#' @export
#'
#' @examples
#' IEATools::sample_iea_data_path() %>%
#'   IEATools::load_tidy_iea_df() %>%
#'   filter_countries_years(countries = c("ZAF"), years = 1960:1999)
filter_countries_years <- function(.df,
                                   countries,
                                   years,
                                   country = IEATools::iea_cols$country,
                                   year = IEATools::iea_cols$year) {
  countries1 <- length(countries) == 1
  years1 <- length(years) == 1
  if (countries1 & years1) {
    if (countries == "all" & years == "all") {
      return(.df)
    }
  }
  if (countries1) {
    if (countries == "all") {
      return(.df %>% dplyr::filter(.data[[year]] %in% years))
    }
  }
  if (years1) {
    if (years == "all") {
      return(.df %>% dplyr::filter(.data[[country]] %in% countries))
    }
  }
  .df %>%
    dplyr::filter(.data[[country]] %in% countries, .data[[year]] %in% years)
}
