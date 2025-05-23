% Generated by roxygen2: do not edit by hand
% Please edit documentation in R/retrieving.R
\name{pl_collect_from_hash}
\alias{pl_collect_from_hash}
\title{Download a data frame based on its \code{pl_hash()}}
\usage{
pl_collect_from_hash(
  hashed_table,
  version_string = NULL,
  decode_fks = TRUE,
  retain_table_name_col = FALSE,
  set_tar_group = TRUE,
  decode_matsindf = TRUE,
  matrix_class = c("Matrix", "matrix"),
  tar_group_colname = PFUPipelineTools::hashed_table_colnames$tar_group_colname,
  matname_colname = PFUPipelineTools::mat_meta_cols$matname,
  matval_colname = PFUPipelineTools::mat_meta_cols$matval,
  valid_from_version_colname = PFUPipelineTools::dataset_info$valid_from_version_colname,
  valid_to_version_colname = PFUPipelineTools::dataset_info$valid_to_version_colname,
  conn,
  schema = schema_from_conn(conn = conn),
  fk_parent_tables = get_all_fk_tables(conn = conn, schema = schema),
  index_map_name = "Index",
  index_map = fk_parent_tables[[index_map_name]],
  rctype_table_name = "matnameRCType",
  rctypes = decode_fks(db_table_name = rctype_table_name, collect = TRUE, conn = conn,
    schema = schema, fk_parent_tables = fk_parent_tables),
  .table_name_col = PFUPipelineTools::hashed_table_colnames$db_table_name,
  .nested_hash_col = PFUPipelineTools::hashed_table_colnames$nested_hash_colname
)
}
\arguments{
\item{hashed_table}{A table created by \code{pl_hash()}.}

\item{version_string}{A string of length 1 indicating the version to be downloaded.
\code{NULL}, the default, means to download all versions.}

\item{decode_fks}{A boolean that tells whether to decode foreign keys
before returning.
Default is \code{TRUE}.}

\item{retain_table_name_col}{A boolean that tells whether to retain the
table name column (\code{.table_name_col}).
Default is \code{FALSE}.}

\item{set_tar_group}{A boolean that tells whether to set the
\code{tar_group_colname} column of the output to
the same value as the input.
There can be only one unique value in \code{tar_group_colname},
otherwise an error is raised.
Default is \code{TRUE}.}

\item{decode_matsindf}{A boolean that tells whether to decode the
a matsindf data frame.
Calls \code{\link[=decode_matsindf]{decode_matsindf()}} internally.
Default is \code{TRUE}.}

\item{matrix_class}{One of "Matrix" (the default for sparse matrices)
or ("matrix") for the native matrix form in \code{R}.
Default is "Matrix".}

\item{tar_group_colname}{The name of the \code{tar_group} column.
default is \code{PFUPipelineTools::hashed_table_colnames$tar_group_colname}.}

\item{matname_colname, matval_colname}{Names used for matrix names and matrix values.
Defaults are
\code{PFUPipelineTools::mat_meta_cols$matname} and
\code{PFUPipelineTools::mat_meta_cols$matval},
respectively.}

\item{valid_from_version_colname, valid_to_version_colname}{Names
for columns containing version information.
Defaults are
\code{PFUPipelineTools::dataset_info$valid_from_version}
and
\code{PFUPipelineTools::dataset_info$valid_to_version},
respectively.}

\item{conn}{The database connection.}

\item{schema}{The database schema (a \code{dm} object).
Default calls \code{schema_from_conn()}, but
you can supply a pre-computed schema for speed.
Needed only when \code{decode_fks = TRUE} (the default).
If foreign keys are not being decoded,
setting \code{NULL} may improve speed.}

\item{fk_parent_tables}{Foreign key parent tables to assist decoding
foreign keys (when \code{decode_fks = TRUE}, the default).
Default calls \code{get_all_fk_tables()}.
Needed only when \code{decode_fks = TRUE} (the default).
If foreign keys are not being decoded,
setting to \code{NULL} may improve speed.}

\item{index_map_name}{The name of the index map.
Default is "Index".}

\item{index_map}{A list of data frames to assist with decoding matrices.
Passed to \code{\link[=decode_matsindf]{decode_matsindf()}} when \code{decode_matsindf} is \code{TRUE}
but otherwise not needed.
Default is \code{fk_parent_tables[[index_map_name]]}.}

\item{rctype_table_name}{The name of the row and column types.}

\item{rctypes}{A data frame of row and column types.
Passed to \code{\link[=decode_matsindf]{decode_matsindf()}} when \code{decode_matsindf} is \code{TRUE}
but otherwise not needed.
Default calls \code{\link[=decode_fks]{decode_fks()}}.}

\item{.table_name_col, .nested_hash_col}{See \code{PFUPipelineTools::hashed_table_colnames}.}
}
\value{
The downloaded data frame described by \code{hashed_table}.
}
\description{
If \code{hashed_table} has \code{0} rows, \code{NULL} is returned.
}
