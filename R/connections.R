#' Create a connection to the Mexer database
#'
#' The process of creating a connection to the Mexer
#' database is cumbersome.
#' This function provides a way to make connections
#' with little fuss and a single line of code.
#'
#' Note, the password for `user` must be set
#' in `.pgpass` or a similar file.
#'
#' The returned connection should be closed by the caller
#' with [DBI::dbDisconnect()].
#'
#' @param dbname The string name of a database at `host`.
#'               Default is "MexerDB" for [get_mexerdb_conn()],
#'               "SandboxDB" for [get_sandboxdb_conn()],
#'               "ScratchMDB" for [get_scratchmdb_conn()], and
#'               "ScratchEDB" for [get_scratchedb_conn()].
#' @param user The string username at `host`.
#'             Default is "dbcreator" for
#'             [get_sandboxdb_conn()],
#'             [get_scratchmdb_conn()], and
#'             [get_scratchedb_conn()]
#'             appropriate for development.
#'             The default user for [get_unit_testing_conn()] is "mkh2".
#' @param host The string site of the database.
#'             Default is "mexer.site".
#' @param port The integer port for the connection.
#'             Default is `6432` ,
#'             the port of the connection pooler, for
#'             [get_mexerdb_conn()],
#'             [get_sandboxdb_conn()],
#'             [get_scratchmdb_conn()], and
#'             [get_scratchedb_conn()].
#'             Default is `5432`, the standard port for
#'             [get_unit_testing_conn()].
#'
#' @return A database connection.
#'
#' @name db-connections
NULL


#' @export
#' @rdname db-connections
get_db_conn <- function(dbname = "MexerDB",
                        user,
                        host = "mexer.site",
                        port = 6432) {
  conn <- DBI::dbConnect(drv = RPostgres::Postgres(),
                         dbname = dbname,
                         host = host,
                         port = port,
                         user = user)
  return(conn)
}


#' @export
#' @rdname db-connections
get_mexerdb_conn <- function(user) {
  get_db_conn(user = user)
}


#' @export
#' @rdname db-connections
get_sandboxdb_conn <- function() {
  get_db_conn(dbname = "SandboxDB",
              user = "dbcreator",
              host = "mexer.site",
              port = 6432)
}


#' @export
#' @rdname db-connections
get_scratchmdb_conn <- function() {
  get_db_conn(dbname = "ScratchMDB",
              user = "dbcreator",
              host = "mexer.site",
              port = 6432)
}


#' @export
#' @rdname db-connections
get_scratchedb_conn <- function() {
  get_db_conn(dbname = "ScratchEDB",
              user = "dbcreator",
              host = "mexer.site",
              port = 6432)
}


#' @export
#' @rdname db-connections
get_unit_testing_conn <- function() {
  get_db_conn(dbname = "unit_testing",
              user = "mkh2",
              host = "mexer.site",
              port = 5432)
}
