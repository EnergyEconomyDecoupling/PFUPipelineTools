## ----include = FALSE----------------------------------------------------------
knitr::opts_chunk$set(
  collapse = TRUE,
  comment = "#>"
)

## ----setup--------------------------------------------------------------------
library(PFUPipelineTools)

## ----connection---------------------------------------------------------------
conn <- DBI::dbConnect(drv = RPostgres::Postgres(),
                       dbname = "MexerDB",
                       host = "mexer.site",
                       port = 6432,
                       user = "dbcreator")
on.exit(DBI::dbDisconnect(conn))

