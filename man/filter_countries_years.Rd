% Generated by roxygen2: do not edit by hand
% Please edit documentation in R/data_functions.R
\name{filter_countries_years}
\alias{filter_countries_years}
\title{Extract specific country and year data}
\usage{
filter_countries_years(
  .df,
  countries,
  years,
  country = IEATools::iea_cols$country,
  year = IEATools::iea_cols$year
)
}
\arguments{
\item{.df}{A data frame containing cleaned data with lots of countries and years.}

\item{countries}{A list of country codes for countries to be analyzed.
"all" means return all countries.}

\item{years}{A vector of years. "all" means return all years.}

\item{country, year}{See \code{IEATools::iea_cols}.}
}
\value{
A data frame with the desired IEA data only.
}
\description{
Data is extracted according to the \code{countries} and \code{years} objects
in a way that is amenable to drake subtargets.
\verb{dplyr:} \verb{:filter()} does the subsetting.
}
\examples{
IEATools::sample_iea_data_path() |>
  IEATools::load_tidy_iea_df() |>
  filter_countries_years(countries = c("ZAF"), years = 1960:1999)
}
