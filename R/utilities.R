#' Self-name a list
#'
#' When using the native pipe operator,
#' it is sometimes convenient to self-name
#' a vector or list,
#' especially for later use in `lapply()` and friends.
#'
#' Internally, this function calls
#' `magrittr::set_names(x, x)`.
#'
#' @param x The character vector or list of characters
#'          to be self-named.
#'
#' @return A self-named version of `x`.
#'
#' @export
#'
#' @examples
#' c("a", "b", "c") |>
#'   self_name()
#' list("A", "B", "C") |>
#'   self_name()
self_name <- function(x) {
  magrittr::set_names(x, x)
}
