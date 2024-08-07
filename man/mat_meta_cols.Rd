% Generated by roxygen2: do not edit by hand
% Please edit documentation in R/data.R
\docType{data}
\name{mat_meta_cols}
\alias{mat_meta_cols}
\title{PSUT matrix formation meta information column names}
\format{
A string list with 6 entries.
\describe{
\item{matname}{The name of the column that contains matrix names.}
\item{matval}{The name of the column in a tidy data frame that contains matrices themselves.}
\item{rowname}{The name of the column that contains matrix row names.}
\item{colname}{The name of the column that contains matrix column names.}
\item{rowtype}{The name of the column that contains matrix row types.}
\item{coltype}{The name of the column that contains matrix column types.}
}
}
\usage{
mat_meta_cols
}
\description{
When forming PSUT matrices, meta information is provided in columns.
This list provides the typical names for the meta information columns
throughout the \code{PFUPipelineTools} package.
}
\examples{
mat_meta_cols
}
\keyword{datasets}
