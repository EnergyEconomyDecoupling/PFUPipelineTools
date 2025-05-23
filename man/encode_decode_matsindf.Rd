% Generated by roxygen2: do not edit by hand
% Please edit documentation in R/utilities.R
\name{encode_decode_matsindf}
\alias{encode_decode_matsindf}
\alias{decode_matsindf}
\alias{encode_matsindf}
\title{Encode and decode \code{matsindf} data frames for storage in a database}
\usage{
decode_matsindf(
  .encoded,
  index_map,
  rctypes,
  wide_by_matrices = TRUE,
  matrix_class = c("matrix", "Matrix"),
  matname = PFUPipelineTools::mat_meta_cols$matname,
  matval = PFUPipelineTools::mat_meta_cols$matval,
  row_index_colname = PFUPipelineTools::mat_colnames$i,
  col_index_colname = PFUPipelineTools::mat_colnames$j,
  value_colname = PFUPipelineTools::mat_colnames$value,
  rowtype_colname = PFUPipelineTools::mat_meta_cols$rowtype,
  coltype_colname = PFUPipelineTools::mat_meta_cols$coltype
)

encode_matsindf(
  .matsindf,
  index_map = magrittr::set_names(list(industry_index_map, product_index_map),
    c(IEATools::row_col_types$industry, IEATools::row_col_types$product)),
  industry_index_map,
  product_index_map,
  retain_zero_structure = FALSE,
  matname = PFUPipelineTools::mat_meta_cols$matname,
  matval = PFUPipelineTools::mat_meta_cols$matval,
  row_index_colname = PFUPipelineTools::mat_colnames$i,
  col_index_colname = PFUPipelineTools::mat_colnames$j,
  value_colname = PFUPipelineTools::mat_colnames$value
)
}
\arguments{
\item{.encoded}{A data frame of matrices in triplet form whose matrices are to be decoded.}

\item{index_map}{A list of two or more index map data frames.
Default is \code{list(Industry = industry_index_map, Product = product_index_map)}.}

\item{rctypes}{A data frame of row and column types.}

\item{wide_by_matrices}{A boolean that tells whether to
\code{\link[tidyr:pivot_wider]{tidyr::pivot_wider()}} the results.
Default is \code{TRUE}.}

\item{matrix_class}{The class of matrices to be created by \code{\link[=decode_matsindf]{decode_matsindf()}}.
One of "matrix" (the default
and \code{R}'s native matrix class) or
"Matrix" (for sparse matrices).}

\item{matname}{The name of the column in \code{.matsindf} that contains matrix names.
Default is "matname".}

\item{matval}{The name of the column in \code{.matsindf} that contains matrix values.
Default is "matval".}

\item{row_index_colname}{The name of the row index column in \code{.encoded}.
Default is "i".}

\item{col_index_colname}{The name of the column index column in \code{.encoded}.
Default is "j".}

\item{value_colname}{The name of the value column.
Default is "value".}

\item{rowtype_colname, coltype_colname}{Names of \code{rowtype} and \code{coltype} columns.}

\item{.matsindf}{A matsindf data frame whose matrices are to be encoded.}

\item{industry_index_map, product_index_map}{Optional data frames with two columns providing the mapping
between row and column indices and row and column names.
See details.}

\item{retain_zero_structure}{A boolean that tells whether to retain
the structure of zero matrices when creating triplets.
Default is \code{FALSE}.
See details.}
}
\value{
For \code{\link[=encode_matsindf]{encode_matsindf()}},
a version of \code{.matsindf} with matrices in triplet form,
appropriate for insertion into a database.
For \code{\link[=decode_matsindf]{decode_matsindf()}},
a version of \code{.encoded} appropriate for in-memory
analysis and calculations.
}
\description{
The CL-PFU database enables storage of \code{matsindf} data frames
by encoding matrix values in triplet format.
These functions perform encoding and decoding
of \code{matsindf} data frames.
\code{\link[=encode_matsindf]{encode_matsindf()}} and \code{\link[=decode_matsindf]{decode_matsindf()}} are inverses of each other.
}
\details{
\code{index_map} must be
an unnamed list of two data frames or
a named list of two or more data frames.
\itemize{
\item If an unnamed list of exactly two data frames,
each data frame must have only
an integer column and a character column.
The first data frame of \code{index_map}
is interpreted as the mapping
between row names and row indices
and
the second data frame of \code{index_map}
is interpreted as the mapping
between column names and column indices.
\item If a named list of two or more data frames,
the names of \code{index_map}
are interpreted as row and column types,
with named data frames applied as the mapping for the
associated row or column type.
For example the data frame named "Industry" would be applied
to the dimension (row or column)
with an "Industry" type.
When both row and column have "Industry" type,
the "Industry" mapping is applied to both.
When sending named data frames in \code{index_map},
matrices to be encoded must have both a row type and a column type.
If an appropriate mapping cannot be found in \code{index_map},
an error is raised.
Both matching data frames must have only
an integer column and
a character column.
}

\code{.matsindf} can be
(a) wide by matrices,
with matrix names as column names or
(b) tidy, with \code{matname} and \code{matval} columns.

If \code{.matsindf} does not contain any matrix columns,
\code{.matsindf} is returned unchanged.

If \code{.encoded} does not contain a \code{matname} column,
\code{.encoded} is returned unchanged.

By default, \code{\link[=encode_matsindf]{encode_matsindf()}} will return
zero-row data frames when
encoding zero matrices.
Set \code{retain_zero_structure = TRUE}
to return all entries in zero matrices.

All of \code{matname}, \code{row_index_colname},
\code{col_index_colname}, and \code{val_colname}
must be present in \code{.encoded}.
If not, \code{.encoded} is returned unmodified.
}
