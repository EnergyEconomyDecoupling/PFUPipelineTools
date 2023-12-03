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
#' IEATools::sample_iea_data_path() |>
#'   IEATools::load_tidy_iea_df() |>
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
      return(.df |> dplyr::filter(.data[[year]] %in% years))
    }
  }
  if (years1) {
    if (years == "all") {
      return(.df |> dplyr::filter(.data[[country]] %in% countries))
    }
  }
  .df |>
    dplyr::filter(.data[[country]] %in% countries, .data[[year]] %in% years)
}


#' Ungroups and removes tar_group column from a data frame
#'
#' The [tarchetypes::tar_group_by()] function
#' adds a column named "tar_group".
#' This function ungroups and removes the special column.
#'
#' @param .df The data frame to be ungrouped.
#' @param tar_group_colname The name of the grouping column. Default is "tar_group".
#' @param ungroup A boolean that tells whether to ungroup `.df`. Default is `TRUE`.
#'
#' @return A modified version of `.df`.
#'
#' @export
tar_ungroup <- function(.df, tar_group_colname = "tar_group", ungroup = TRUE) {
  out <- .df |>
    dplyr::mutate(
      "{tar_group_colname}" := NULL
    )
  if (ungroup) {
    out <- out |>
      dplyr::ungroup()
  }
  return(out)
}



#' Read a version of a pinned CL-PFU database product
#'
#' @param pin_name The string name of the pin to be read.
#' @param database_version A string, prefixed with "v" for the version of interest.
#'                         Any number will be prefixed by "v" and converted to a string internally.
#' @param pin_version_string The version string for pin `pin_name` associated with `database_version`.
#'                           Default is `pin_versions(database_version)[[pin_name]]`.
#' @param pipeline_releases_folder The path to the pipeline releases folder.
#'                                 Default is `get_abs_paths()[["pipeline_releases_folder"]]`.
#'
#' @return The pinned object represented by the name and the version string.
#'
#' @export
#'
#' @examples
#' \dontrun{
#' read_pin_version(pin_name = "phi_vecs", database_version = 1.2) |>
#'   head()
#'}
read_pin_version <- function(pin_name,
                             database_version,
                             pin_version_string = PFUSetup::pin_versions(database_version)[[pin_name]],
                             pipeline_releases_folder = PFUSetup::get_abs_paths()[["pipeline_releases_folder"]]) {

  pinboard <- pipeline_releases_folder |>
    pins::board_folder(versioned = TRUE)
  # Check if the pin exists
  if (! pins::pin_exists(pinboard, name = pin_name)) {
    stop(paste("CL-PFU database product", pin_name, "does not exist."))
  }

  pinboard |>
    pins::pin_read(name = pin_name, version = pin_version_string)
}
