#' Country abbreviations
#'
#' A string list containing all known countries in abbreviated format.
#' 3-letter codes are analyzed as separate countries.
#' The 3-letter codes conform to ISO naming conventions.
#' 4-letter codes are not ISO codes but still analyzed as separate countries.
#' 5-letter codes (and longer) are aggregations.
#'
#' @format A string list with `r length(all_countries)` entries.
#'
#' @examples
#' all_countries
"all_countries"


#' Double-counted countries
#'
#' A string list containing abbreviated names of double-counted countries.
#'
#' @format A string list with `r length(double_counted_countries)` entries.
#'
#' @examples
#' double_counted_countries
"double_counted_countries"


#' Canonical countries
#'
#' A string list containing abbreviated names of canonical countries.
#' Canonical countries is the set difference between
#' `all_countries` and `double-counted_countries`.
#'
#' @format A string list with `r length(canonical_countries)` entries.
#'
#' @examples
#' canonical_countries
"canonical_countries"


#' Key column info
#'
#' A string list containing information about database table keys.
#'
#' @format A string list with `r length(key_col_info)` entries.
#'
#' @examples
#' key_col_info
"key_col_info"
