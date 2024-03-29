#' Save a target to a pinboard.
#'
#' Releases (`release = TRUE`)
#' or not (`release = FALSE`)
#' a new version of `targ`
#' using the `pins` package.
#'
#' Released versions of the target can be obtained
#' as shown in examples.
#'
#' @param pipeline_releases_folder The folder that contains the pinboard for releases from the pipeline.
#' @param targ The target R object to be saved to the pinboard.
#' @param pin_name The name of the pin in the pinboard. `pin_name` is the key to retrieving `targ`.
#' @param type The type of the target, routed to `pins::pin_write()`. Default is "rds". "csv" in another option.
#' @param release A boolean telling whether to do a release.
#'                Default is `FALSE`.
#'
#' @return If `release` is `TRUE`,
#'         the fully-qualified path name of the `targ` file in the pinboard.
#'         If `release` is `FALSE`, the string "Release not requested."
#'
#' @export
#'
#' @examples
#' \dontrun{
#' # Establish the pinboard
#' pinboard <- file.path("~",
#'                       "Dropbox",
#'                       "Fellowship 1960-2015 PFU database",
#'                       "OutputData", "PipelineReleases") |>
#'   pins::board_folder()
#' # Get information about the `PSUT` target in the pinboard
#' pinboard |>
#'   pins::pin_meta(name = "psut")
#' # Find versions of the `PSUT` target
#' pinboard |>
#'   pins::pin_versions(name = "psut")
#' # Get the latest copy of the `PSUT` target.
#' my_psut <- pinboard |>
#'   pins::pin_read(name = "psut")
#' # Retrieve a previous version of the `PSUT` target.
#' my_old_psut <- pinboard |>
#'   pins::pin_read(name = "psut", version = "20220218T023112Z-1d9e1")
#' }
release_target <- function(pipeline_releases_folder, targ, pin_name, type = "rds", release = FALSE) {
  if (release) {
    # Establish the pinboard
    out <- pins::board_folder(pipeline_releases_folder, versioned = TRUE) |>
      # Returns the fully-qualified name of the file written to the pinboard.
      pins::pin_write(targ, name = pin_name, type = type, versioned = TRUE)
  } else {
    out <- "Release not requested."
  }
  return(out)
}


#' Calculate hash of pipeline data
#'
#' In the CL-PFU database pipeline,
#' we need the ability to download a
#' data frame from the database
#' based on a hash of the data.
#' This function calculates the appropriate hash.
#'
#' The hash has two requirements:
#'
#'   - values change when content changes and
#'   - provides sufficient information to retrieve the
#'     data frame from the database.
#'
#' The return value from this function
#' (being a special hash of a database table)
#' serves as a "ticket" with which
#' data can be retrieved from the database at a later time using
#' [pl_collect_from_hash()].
#'
#' To meet the requirements of the hash,
#' the return value from this function
#' has the following characteristics:
#'
#'   - The first column (named with the value of `.table_name_col`)
#'     contains the value of `table_name`, the name
#'     of the database table where the actual data frame is stored.
#'   - The last column (at the right and named with the value of `.nested_col`)
#'     contains a hash of a data frame created by nesting
#'     by all columns with more than one unique value and
#'     `additional_hash_group_cols`
#'     (when `additional_hash_group_cols` is not `NULL`).
#'   - The second through N-1 columns are
#'     all columns with only one unique value
#'     (provided that `keep_single_unique_cols` is `TRUE` AND
#'     those columns specified by
#'     `additional_hash_group_cols`
#'     (provided that `additional_hash_group_cols` is not `NULL`, the default).
#'
#' If `keep_single_unique_cols` is `FALSE` and `additional_hash_group_cols` is `NULL`,
#' an error is raised.
#'
#' Hashes can be created from data frames in memory,
#' typically about to be uploaded to the database.
#' To do so, supply `.df` as a data frame.
#' If the `.table_name_column` is not present in `.df`,
#' it is added internally, filled with the value of `table_name`.
#'
#' Alternatively, hashes can be created from a table
#' already existing in the database at `conn`.
#' To do this, leave `.df` at its default value (`NULL`) and supply
#' the `table_name` and `conn` arguments.
#' In this case,
#' an SQL query is generated and a hash of the entire table is provided
#' as the return value.
#' `.table_name_column` is added to the result after downloading.
#'
#' Both approaches use the `md5` hashing algorithm.
#'
#' That said, the two approaches do not give the same hashes
#' for the same data frame, due to differences
#' in the way that the database creates its hash vs. how R creates its hash.
#'
#' @param .df An in-memory data frame to be stored in the database or `NULL` if
#'            the has of a table in the database at `conn` is desired.
#' @param table_name The string name of the table in which `.df` will be stored
#'                   or the name of a table in the database to be hashed.
#' @param conn A connection to a database.
#'             Necessary only if `.df` is `NULL` (its default value).
#' @param additional_hash_group_cols A string vector of names of
#'                                   additional columns by which `.df` will be grouped
#'                                   before making the `.nested_hash_col` hash column.
#'                                   All `additional_hash_group_cols` that exist in the
#'                                   data frame or table being hashed
#'                                   will be present in the result.
#'                                   Default is `NULL`, meaning that grouping will be
#'                                   done on all columns that contain only 1 unique value.
#'                                   See details.
#' @param usual_hash_group_cols The string vector of usual column names to be preserved
#'                              in the hashed data frame.
#'                              Default is `PFUPipelineTools::usual_hash_group_cols`.
#' @param keep_single_unique_cols A boolean that tells whether to keep columns
#'                                that have a single unique value in the outgoing hash.
#'                                Default is `TRUE`.
#' @param .table_name_col The name of the column of the output that contains `table_name`.
#'                        Default is `PFUPipelineTools::hashed_table_colnames$db_table_name`.
#' @param .nested_hash_col The name of the column of the output that contains
#'                         the hash of nested columns.
#'                         Default is `PFUPipelineTools::hashed_table_colnames$nested_hash_colname`.
#' @param .nested_col The name of the column of the output that contains
#'                    nested data.
#'                    Used internally.
#'                    Default is `PFUPipelineTools::hashed_table_colnames$nested_colname`.
#' @param .algo The algorithm for hashing.
#'              Default is "md5".
#'
#' @return A data frame "ticket" for later retrieving data from the database.
#'
#' @export
pl_hash <- function(.df = NULL,
                    table_name,
                    conn,
                    additional_hash_group_cols = NULL,
                    keep_single_unique_cols = TRUE,
                    usual_hash_group_cols = PFUPipelineTools::usual_hash_group_cols,
                    .table_name_col = PFUPipelineTools::hashed_table_colnames$db_table_name,
                    .nested_hash_col = PFUPipelineTools::hashed_table_colnames$nested_hash_colname,
                    .nested_col = PFUPipelineTools::hashed_table_colnames$nested_colname,
                    .algo = "md5") {
  if (!is.null(table_name)) {
    # Make sure the table_name has length 1.
    if (length(table_name) != 1) {
      stop("length(table_name) must be 1 in pl_hash()")
    }
  }

  if (is.null(additional_hash_group_cols) & is.null(usual_hash_group_cols) & !keep_single_unique_cols) {
    stop("additional_hash_group_cols was `NULL`, `usual_hash_group_cols` was `NULL`, and keep_single_unique_cols was `TRUE` in pl_hash(). Something must change!")
  }

  if (!is.null(.df)) {
    # We have an in-memory data frame
    if (!(table_name %in% colnames(.df))) {
      # Set the table name column
      .df <- .df |>
        dplyr::mutate(
          "{.table_name_col}" := table_name,
        ) |>
        # Move the table name column to the left of the data frame
        dplyr::relocate(dplyr::all_of(.table_name_col))
    }

    # Start with an empty character vector. Fill as we go along.
    hash_group_cols <- usual_hash_group_cols

    if (keep_single_unique_cols) {
      # Figure out names of columns that have only 1 unique value.
      # We will group by these columns before nesting and hashing.
      single_value_cols <- .df |>
        sapply(function(this_col) {
          length(unique(this_col)) == 1
        })
      hash_group_cols <- hash_group_cols |>
        append(names(single_value_cols[single_value_cols])) |>
        unique()
    }

    if (!is.null(additional_hash_group_cols)) {
      hash_group_cols <- hash_group_cols |>
        append(additional_hash_group_cols) |>
        unique()
    }
    # Looks like there is a bug in tidyr::nest().
    # When nesting with a named vector in group_by(),
    # the nested data frame picks up the names of the vector.
    # To work around the bug, unname hash_group_cols here.
    unnamed_hash_group_cols <- unname(hash_group_cols)
    out <- .df |>
      # Group by hash_group_cols
      dplyr::group_by(dplyr::across(dplyr::any_of(unnamed_hash_group_cols))) |>
      # Nest
      tidyr::nest(.key = .nested_col) |>
      # Calculate hash
      dplyr::mutate(
        "{.nested_hash_col}" := digest::digest(.data[[.nested_col]], algo = .algo),
        "{.nested_col}" := NULL
      ) |>
      dplyr::ungroup()


    # With the code above,
    # The hashes for the same data frame done two different ways
    # (one in memory and supplied in .df,
    # the other calculated inboard from conn and table_name)
    # are different.
    # It may not be essential that the hashes are the same.
    # Below is some alternative code to hash the in-memory data frame
    # that is meant to mimic the way the inboard calculations work,
    # namely, converting to strings before doing the hashing.
    # But even the commented code below doesn't seem to work.
    #
    # convert_numeric_to_text <- function(df) {
    #   df |>
    #     dplyr::mutate(
    #       dplyr::across(dplyr::where(is.numeric), as.character)
    #     )
    # }
    #
    # concatenate_columns <- function(df) {
    #   # df |>
    #   #   tidyr::unite("Concatenated_Columns", dplyr::everything(), sep = ", ")
    #   df |>
    #     as.list() |>
    #     unname()
    # }
    #
    # out <- .df |>
    #   # Group by hash_group_cols
    #   dplyr::group_by(dplyr::across(dplyr::all_of(hash_group_cols))) |>
    #   # Nest
    #   tidyr::nest(.key = .nested_col) |>
    #   dplyr::mutate(
    #     dplyr::across(.nested_col, ~ purrr::map(.x, convert_numeric_to_text))
    #   ) |>
    #   dplyr::mutate(
    #     dplyr::across(.nested_col, ~ purrr::map(.x, concatenate_columns)),
    #     "{.nested_col}" := digest::digest(.data[[.nested_col]], algo = .algo)
    #   ) |>
    #   tidyr::unnest(cols = .nested_col)

  } else {
    # We need to retrieve a tbl reference to a data frame in a database.
    .df <- dplyr::tbl(conn, table_name)

    # Build the SQL statement that will pull down the hashed table

    # Start with an empty character vector for cols_to_nest
    cols_to_keep <- usual_hash_group_cols

    if (keep_single_unique_cols) {
      # First find the columns with only 1 unique value.
      # These are columns to nest, unless
      # they are in additional_hash_group_cols.
      single_unique_cols <- unique_cols_in_tbl(table_name = table_name, conn = conn)
      cols_to_keep <- cols_to_keep |>
        append(single_unique_cols) |>
        unique()
    }
    if (!is.null(additional_hash_group_cols)) {
      cols_to_keep <- cols_to_keep |>
        append(additional_hash_group_cols) |>
        unique()
    }
    # We want to keep the cols with only one unique value AND
    # the additional_hash_group_cols but only if they are in colnames(.df).
    cols_to_keep <- cols_to_keep[cols_to_keep %in% colnames(.df)]

    # The columns to nest are everything else.
    cols_to_nest <- setdiff(colnames(.df), cols_to_keep)

    # Build the query to return a hashed table

    # Here is an example query

    # SELECT
    # "Country",
    # "EnergyType",
    # md5(array_agg("Year")::text || array_agg("Value")::text) AS "NestedHashColumn"
    # FROM
    # "TestPLHash"
    # GROUP BY
    # ("Country", "EnergyType");

    keep_cols_clause <- paste0('"', cols_to_keep, '"', collapse = ', ')
    select_clause <- paste0('SELECT ', keep_cols_clause, ', ')

    nest_cols_clause_temp <- paste0('array_agg("', cols_to_nest, '")::text', collapse = ' || ')

    nest_cols_clause <- paste0('md5(',
                               nest_cols_clause_temp,
                               ') AS "',
                               .nested_hash_col,
                               '" ')

    from_clause <- paste0('FROM "', table_name, '" ')

    group_clause <- paste0('GROUP BY (', keep_cols_clause, ');')

    stmt <- paste0(select_clause, nest_cols_clause, from_clause, group_clause)

    out <- DBI::dbGetQuery(conn, stmt) |>
      tibble::as_tibble() |>
      dplyr::mutate(
        # Add the table name so we can retrieve later
        "{.table_name_col}" := table_name,
      )  |>
      # Move the table name column to the left of the data frame
      dplyr::relocate(dplyr::all_of(.table_name_col))
  }

  return(out)
}


unique_cols_in_tbl <- function(table_name, conn) {
  # Get the tbl
  this_tbl <- dplyr::tbl(conn, table_name)
  # The SQL query looks like this:

  # SELECT
  # COUNT(DISTINCT "Country") AS UniqueCountries,
  # COUNT(DISTINCT "Year") AS UniqueYears,
  # COUNT(DISTINCT "EnergyType") AS UniqueEnergyTypes,
  # COUNT(DISTINCT "Value") AS UniqueValues
  # FROM
  # "TestPLHash";

  # Get the count clause based on the the column names in the table
  count_clause <- colnames(this_tbl) |>
    sapply(FUN = function(cname){
      paste0('COUNT(DISTINCT "', cname, '") AS "', cname, '" ')
    }) |>
    paste0(collapse = ', ')
  # Build the SQL query
  query <- paste0('SELECT ', count_clause, ' FROM "', table_name, '"')
  # Get a 1-row data frame with existing columns and the number of unique values
  count_df <- DBI::dbGetQuery(conn, query)
  # Finally, get the names of the columns where the value is 1
  names(count_df)[count_df == 1]
}





















