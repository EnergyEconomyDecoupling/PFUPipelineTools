% Generated by roxygen2: do not edit by hand
% Please edit documentation in R/compress.R
\name{remove_compress_function}
\alias{remove_compress_function}
\title{Remove the compress function from a database}
\usage{
remove_compress_function(conn)
}
\arguments{
\item{conn}{A connection to the database from which the \code{compress}
function should be removed.}
}
\value{
The number of rows affected by adding the function to the
database at \code{conn},
which should always be \code{0}.
}
\description{
The database at \code{conn} may have the \code{compress} function installed
by \code{unpload_compress_function()}.
This \code{R} function removes the \code{compress} function from the database at \code{conn}.
}
\details{
The function can be removed only by the owner of the function,
i.e., the user who installed the function in the first instance.
}
\seealso{
\code{\link[=install_compress_function]{install_compress_function()}}, \code{\link[=compress_rows]{compress_rows()}}
}
