#' Upsert a data frame with optional encoding of foreign keys
#'
#' Upserts
#' (inserts or updates,
#' depending on whether the private keys in `.df`
#' already exist in `db_table_name`)
#' `.df` into `db_table_name` at `conn`.
#'
#' This function decodes foreign keys (fks), when possible,
#' assuming that all fks are integers.
#' If non-integers (typically, character strings)
#' are provided in fk columns of `.df`,
#' the non-integers will be recoded to their appropriate integer key values.
#'
#' This function knows about CL-PFU database tables that contain
#' matrix information.
#' In particular, if `.df` contains matrices,
#' they are expanded into row-col-val format
#' before uploading.
#'
#' The output of this function is a special data frame that
#' contains the following columns:
#'
#'   * All single-valued columns columns in `.df`,
#'     columns given in `additional_hash_group_cols`
#'     (default `NULL`), and
#'     columns given in `usual_hash_group_cols`
#'     (default `PFUPipelineTools::usual_hash_group_cols`).
#'   * Hash: A column with a hash of all non-foreign-key columns.
#'
#' `schema` is a data model (`dm` object) for the database in `conn`.
#' Its default value (`schema_from_conn(conn)`)
#' extracts the data model for the database at `conn` automatically.
#' However, if the caller already has the data model,
#' supplying it in the `schema` argument will save time.
#'
#' `fk_parent_tables` is a named list of tables,
#' one of which (the one named `db_table_name`)
#' contains the foreign keys for `db_table_name`.
#' `fk_parent_tables` is treated as a store from which foreign key tables
#' are retrieved by name when needed.
#' The default value (which calls `get_all_fk_tables()`
#' with `collect = TRUE` because decoding of foreign keys
#' is done outboard of the database)
#' retrieves all possible foreign key parent tables from `conn`,
#' potentially a time-consuming process.
#' For speed, pre-compute all foreign key parent tables once
#' (via `get_all_fk_tables(collect = TRUE)`)
#' and pass the list to the `fk_parent_tables` argument
#' of this function.
#'
#' The user in `conn` must have write access to the database.
#'
#' By default, [pl_upsert()] will delete all zero entries
#' in matrices before upserting.
#' But for some countries and years,
#' that could result in missing matrices, such as **U_EIOU**.
#' Set `retain_zero_structure = TRUE`
#' to preserve all entries in a zero matrix.
#'
#' @param .df The data frame to be upserted.
#' @param conn A connection to the CL-PFU database.
#' @param db_table_name A string identifying the destination for `.df` in `conn`,
#'                      i.e. the name of a remote database table.
#'                      Default is `NULL`, meaning that the value for this argument
#'                      will be taken from the `.db_table_name` column of `.df`.
#' @param additional_hash_group_cols A vector or list of additional columns
#'                                   by which `.df` will be grouped
#'                                   before hashing and, therefore, appear in the output.
#'                                   Default is `NULL`.
#'                                   Passed to [pl_hash()].
#' @param usual_hash_group_cols A vector of columns by which `.df` will be grouped
#'                              before hashing and, therefore, appear in the output.
#'                              Default is `PFUPipelineTools::additional_hash_group_cols`
#'                              but can be set to `NULL` to disable.
#'                              Passed to [pl_hash()].
#' @param keep_single_unique_cols A boolean that tells whether to keep
#'                                columns with a single unique value
#'                                in the output.
#'                                Default is `TRUE`.
#'                                Passed to [pl_hash()].
#' @param in_place A boolean that tells whether to modify the database at `conn`.
#'                 Default is `FALSE`, which is helpful if you want to chain
#'                 several requests.
#' @param encode_fks A boolean that tells whether to code foreign keys in `.df`
#'                   before upserting to `conn`.
#'                   Default is `TRUE`.
#' @param compress A boolean that tells whether to compress `db_table_name`
#'                 in the database after uploading.
#'                 Default is `FALSE`.
#' @param round_double_columns A boolean that tells whether to
#'                             round double-precision columns in `.df`.
#'                             Default is `FALSE`.
#' @param digits An integer that tells the number of significant digits.
#'               `digits` has an effect only when `round_double_columns` is `TRUE`.
#'               Default is `15`, which should
#'               eliminate any numerical precision errors
#'               for [compress_rows()].
#' @param index_map A list of 2 or more data frames that represent the
#'                  mappings from inboard row and column indices in the database
#'                  to outboard row and column names in the memory
#'                  of the local computer.
#'                  See documentation for [encode_matsindf()] and
#'                  [matsbyname::to_triplet()].
#'                  Default is a `list` that contains the `industry`, `product`, and `other`
#'                  members of `fk_parent_tables`.
#' @param retain_zero_structure A boolean that tells whether to retain the structure
#'                              of zero matrices.
#'                              See details.
#' @param schema The data model (`dm` object) for the database in `conn`.
#'               Default is `dm_from_con(conn, learn_keys = TRUE)`.
#'               See details.
#' @param fk_parent_tables A named list of all parent tables
#'                         for the foreign keys in `db_table_name`.
#'                         See details.
#' @param .db_table_name The name of the table name column in `.df`.
#'                       Default is `PFUPipelineTools::hashed_table_colnames$db_table_name`.
#' @param .pk_col The name of the primary key column in a primary key table.
#'                See `PFUPipelineTools::dm_pk_colnames`.
#' @param .algo The hashing algorithm.
#'              Default is "md5".
#'
#' @return A hash of `.df` according to `.algo`.
#'
#' @seealso `pl_download()` for the reverse operation.
#'          `pl_upload_schema_and_simple_tables()` for a way to establish the database schema.
#'
#' @export
pl_upsert <- function(.df,
                      conn,
                      db_table_name = NULL,
                      additional_hash_group_cols = NULL,
                      usual_hash_group_cols = PFUPipelineTools::usual_hash_group_cols,
                      keep_single_unique_cols = TRUE,
                      in_place = FALSE,
                      encode_fks = TRUE,
                      compress = FALSE,
                      round_double_columns = FALSE,
                      digits = 15,
                      index_map = list(fk_parent_tables[[IEATools::row_col_types$industry]],
                                       fk_parent_tables[[IEATools::row_col_types$product]],
                                       fk_parent_tables[[IEATools::row_col_types$other]]) |>
                        magrittr::set_names(c(IEATools::row_col_types$industry,
                                              IEATools::row_col_types$product,
                                              IEATools::row_col_types$other)),
                      retain_zero_structure = FALSE,
                      schema = schema_from_conn(conn),
                      fk_parent_tables = get_all_fk_tables(conn = conn,
                                                           schema = schema,
                                                           collect = TRUE),
                      .db_table_name = PFUPipelineTools::hashed_table_colnames$db_table_name,
                      .pk_col = PFUPipelineTools::dm_pk_colnames$pk_col,
                      .algo = "md5") {

  if (is.null(db_table_name)) {
    db_table_name <- .df[[.db_table_name]] |>
      unique()
  }
  if (length(db_table_name) != 1) {
    stop("length(db_table_name) must be 1 in pl_upsert()")
  }

  # Eliminate the .db_table_name column if it exists.
  # We don't upload the table with that column.
  .df <- .df |>
    dplyr::mutate(
      "{.db_table_name}" := NULL
    )

  pk_table <- dm::dm_get_all_pks(schema, table = dplyr::all_of({{db_table_name}}))
  # Make sure we have one and only one primary key row
  assertthat::assert_that(nrow(pk_table) == 1,
                          msg = paste0("Table '",
                                       db_table_name,
                                       "' has ", nrow(pk_table),
                                       " primary keys. 1 is required."))
  # Get the primary key name as a string for later use in the upsert command
  pk_str <- pk_table |>
    # .pk_col is the name of the column of primary key names
    # in the tibble returned by dm::dm_get_all_pks()
    magrittr::extract2(.pk_col) |>
    magrittr::extract2(1)

  df_matsindf_encoded <- .df |>
    # Encode for upload using the index_map
    encode_matsindf(index_map = index_map,
                    retain_zero_structure = retain_zero_structure)

  # Encode fk column values in .df with integer keys, if requested.
  df_to_upsert <- df_matsindf_encoded |>
    # The database shouldn't care about targets groups, so
    # remove any targets grouping.
    tar_ungroup()
  if (encode_fks) {
    df_to_upsert <- df_to_upsert |>
      encode_fks(db_table_name = db_table_name,
                 schema = schema,
                 fk_parent_tables = fk_parent_tables)
  }

  # Round double-precision columns, if desired
  if (round_double_columns) {
    df_to_upsert <- df_to_upsert |>
      round_double_cols(digits = digits)
  }

  # Perform the upload.
  dplyr::tbl(conn, db_table_name) |>
    dplyr::rows_upsert(df_to_upsert,
                       by = pk_str,
                       copy = TRUE,
                       in_place = in_place)

  # Compress the table, if desired.
  if (compress) {
    compress_rows(db_table_name = db_table_name, conn = conn)
  }

  # Return a hash of df_matsindf_encoded
  df_matsindf_encoded |>
    pl_hash(table_name = db_table_name,
            keep_single_unique_cols = keep_single_unique_cols,
            additional_hash_group_cols = additional_hash_group_cols,
            usual_hash_group_cols = usual_hash_group_cols)
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
#' @param tar_group_col The name of the tar_group column.
#'                      Default is "tar_group".
#' @param .algo The algorithm for hashing.
#'              Default is "md5".
#'
#' @return A data frame "ticket" for later retrieving data from the database.
#'
#' @export
pl_hash <- function(.df = NULL,
                    table_name,
                    conn,
                    usual_hash_group_cols = PFUPipelineTools::usual_hash_group_cols,
                    additional_hash_group_cols = NULL,
                    keep_single_unique_cols = TRUE,
                    .table_name_col = PFUPipelineTools::hashed_table_colnames$db_table_name,
                    .nested_hash_col = PFUPipelineTools::hashed_table_colnames$nested_hash_colname,
                    .nested_col = PFUPipelineTools::hashed_table_colnames$nested_colname,
                    tar_group_col = "tar_group",
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





















