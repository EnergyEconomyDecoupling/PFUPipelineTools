#' Trim a pipeline to desired length
#'
#' When constructing pipelines,
#' it can be desirable to keep only a few of the targets.
#' This function trims the pipeline to the last desired target.#'
#'
#' @param .pipeline The pipeline to trim. A list.
#' @param last_target_to_keep The last target to keep in the pipeline.
#'
#' @return `.pipeline` with targets _after_ `last_target_to_keep` removed.
#'
#' @export
trim_pipeline <- function(.pipeline, last_target_to_keep) {
  last_index_to_keep <- which(names(.pipeline) == last_target_to_keep)
  assertthat::assert_that(length(last_index_to_keep) == 1)
  .pipeline[1:last_index_to_keep]
}
