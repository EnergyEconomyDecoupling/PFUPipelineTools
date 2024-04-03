#' Country abbreviations
#'
#' A string list containing all known countries in abbreviated format.
#' 3-letter codes are analyzed as separate countries.
#' The 3-letter codes conform to ISO naming conventions.
#' 4-letter codes are not ISO codes but still analyzed as separate countries.
#' 5-letter codes (and longer) are aggregations.
#'
#' @format A string list with `r length(all_countries)` entries.
#'
#' @examples
#' all_countries
"all_countries"


#' Double-counted countries
#'
#' A string list containing abbreviated names of double-counted countries.
#'
#' @format A string list with `r length(double_counted_countries)` entries.
#'
#' @examples
#' double_counted_countries
"double_counted_countries"


#' Canonical countries
#'
#' A string list containing abbreviated names of canonical countries.
#' Canonical countries is the set difference between
#' `all_countries` and `double-counted_countries`.
#'
#' @format A string list with `r length(canonical_countries)` entries.
#'
#' @examples
#' canonical_countries
"canonical_countries"


#' Metadata about table keys
#'
#' A string list containing information about key columns.
#'
#' @format A string list with `r length(key_col_info)` entries.
#' \describe{
#' \item{pk_suffix}{The string suffix for names of key columns, "ID".}
#' }
#'
#' @examples
#' key_col_info
"key_col_info"


#' Column names in schema tables
#'
#' A string list names of columns in schema tables.
#'
#' @format A string list with `r length(schema_table_colnames)` entries.
#' \describe{
#' \item{table}{The name of a string column that tells the database table name.}
#' \item{colname}{The name of a string column that identifies a column in `table`.}
#' \item{is_pk}{The name of a boolean column that tells whether `column` is a primary key.}
#' \item{coldatatype}{The name of a string column that tells the data type of `column`, such as "int", "text", "boolean", or "double precision".}
#' \item{fk_table}{The name of a string column that identifies the table in which a foreign key can be found. "NA" is a valid value for `fk_table`, meaning that `colname` does not have a foreign key.}
#' \item{fk_colname}{The name of a string column that identifies the name of a column in `fk_table` that contains values for the foreign key. "NA" is a valid value for `fk_colname`, meaning that `colname` does not have a foreign key.}
#' }
#'
#' @examples
#' schema_table_colnames
"schema_table_colnames"


#' Column names in primary key tables from `dm`
#'
#' `dm_get_all_pks()` from the package `dm`
#' returns a data frame with several columns.
#' This constant gives those names as a string list
#' to document their meaning and
#' to guard against future changes.
#'
#' @format A string list with `r length(dm_pk_colnames)` entries.
#' \describe{
#' \item{table}{The name of a string column that tells table names.}
#' \item{pk_col}{The name of a string column that identifies primary key columns in `table`.}
#' \item{autoincrement}{The name of a boolean column that tells whether `pk_col` is auto-incrementing.}
#' }
#'
#' @examples
#' dm_pk_colnames
"dm_pk_colnames"


#' Column names in foreign key tables from `dm`
#'
#' `dm_get_all_fks()` from the package `dm`
#' returns a data frame with several columns.
#' This constant gives those names as a string list
#' to document their meaning and
#' to guard against future changes.
#'
#' In this context, a "parent" table contains the foreign key definition
#' and its values.
#' A "child" table contains at least one foreign column that references
#' a column in the parent table.
#'
#' @format A string list with `r length(dm_fk_colnames)` entries.
#' \describe{
#' \item{child_table}{The name of a string column that tells child table names.}
#' \item{child_fk_cols}{The name of a string column that identifies foreign key columns in the child table.}
#' \item{parent_table}{The name of a string column that identifies the table where the child's foreign key is defined.}
#' \item{parent_key_cols}{The name of a string column that tells which column in the parent table contains the definition of the foreign key.}
#' \item{on_delete}{Tells what will happen when the foreign key column is deleted.}
#' }
#'
#' @examples
#' dm_fk_colnames
"dm_fk_colnames"


#' Column names in hashed tables
#'
#' A hashed table includes a column that contains
#' a string identifying the database table
#' in which the data are stored.
#' This object stores the name of that column.
#'
#' @format A named list with `r length(hashed_table_colnames)`
#'         entries of hashed table column names.
#'
#' @examples
#' hashed_table_colnames
"hashed_table_colnames"


#' Example database schema table
#'
#' A data frame containing several columns
#' that describe a database schema
#' for simple facts about the Beatles.
#'
#' @format A data frame with columns "Table", "colname", "coldatatype", "fk.table", and "fk.colname".
#'
#' @examples
#' beatles_schema_table
"beatles_schema_table"


#' Example simple database tables
#'
#' A named list of data frames, each of which
#' is a foreign key table (fk table)
#' with simple information about the Beatles.
#'
#' This list is in the correct format
#' for the function `pl_upload_schema_and_simple_tables()`.
#'
#' @format A named list of data frames.
#'
#' @examples
#' beatles_fk_tables
"beatles_fk_tables"


#' Information about the machine efficiency files
#'
#' A string list containing information about machine efficiency files.
#' Items in the list provide default values for machine efficiency files,
#' including Excel tab names, etc.
#'
#' @format A string list with `r length(machine_constants)` entries.
#' \describe{
#' \item{efficiency_tab_name}{The default name of the efficiency tabs in machine efficiency excel files.}
#' }
#'
#' @examples
#' machine_constants
"machine_constants"


#' Constants for data frames containing IEA and MW data frames
#'
#' A string list containing both the column name and column values
#' for energy conversion chains (ECCs) in PSUT format.
#'
#' @format A string list with `r length(ieamw_cols)` entries.
#' \describe{
#' \item{ieamw}{The name of the column containing metadata on ECC sources. "IEAMW"}
#' \item{iea}{A string identifying that ECC data are from the IEA exclusively. "IEA"}
#' \item{mw}{A string identifying that ECC data are for muscle work (MW) exclusively. "MW"}
#' \item{both}{A string identifying that ECC data include both IEA and muscle work. "Both"}
#' }
#'
#' @examples
#' ieamw_cols
"ieamw_cols"


#' Exemplar table names
#'
#' A string list containing named names of columns and tabs for exemplar tables.
#' Items in the list provide default values for column name function arguments
#' throughout the `PFUPipeline` package.
#'
#' @format A string list with `r length(exemplar_names)` entries.
#' \describe{
#' \item{exemplar_tab_name}{The string name of the tab in the Excel file containing the exemplar table.}
#' \item{prev_names}{The name of a column of previous names used for the country.}
#' \item{exemplars}{The name of a column of exemplar countries.}
#' \item{exemplar_country}{The name of an exemplar country column.}
#' \item{exemplar_countries}{The name of an exemplar countries column.}
#' \item{exemplar_tables}{The name of a column containing exemplar tables.}
#' \item{iea_data}{The name of a column containing IEA extended energy balance data.}
#' \item{alloc_data}{The name of a column containing final-to-useful allocation data.}
#' \item{incomplete_alloc_table}{The name of a column containing incomplete final-to-useful allocation tables.}
#' \item{complete_alloc_table}{The name of a column containing completed final-to-useful allocation tables.}
#' \item{incomplete_eta_table}{The name of a column containing incomplete final-to-useful efficiency tables.}
#' \item{complete_eta_table}{The name of a column containing completed final-to-useful efficiency tables.}
#' \item{region_code}{The name of the region code column.}
#' \item{country_name}{The name of the column containing the long name of a country.}
#' \item{agg_code_col}{The metadata column "Agg.Code", representing the country, or country group code for individual country level data to be aggregated in to.}
#' \item{world}{The name of the world region.}
#' }
#'
#' @examples
#' exemplar_names
"exemplar_names"


#' Sources for phi values
#'
#' A string list containing named sources of phi (exergy-to-energy ratio) values.
#'
#' @format A string list with `r length(phi_sources)` entries.
#' \describe{
#' \item{eta_fu_tables}{Tables of final-to-useful efficiency values.}
#' \item{temperature_data}{Country-average yearly temperature data.}
#' \item{phi_constants}{Tables of constant phi values.}
#' }
#'
#' @examples
#' phi_sources
"phi_sources"


#' Column name for datasets
#'
#' A string list containing the column name for datasets.
#'
#' @format A string list with `r length(dataset_info)` entries.
#' \describe{
#' \item{dataset_colname}{The string name of the dataset column, "Dataset".}
#' }
#'
#' @examples
#' dataset_info
"dataset_info"


#' Usual columns to be preserved when hashing target uploads
#'
#' A string list containing names of the usual columns to preserve on upload,
#' in addition to columns that contain only one unique value.
#'
#' @format A string list with `r length(usual_hash_group_cols)` entries.
#' \describe{
#' \item{dataset}{The string name of the dataset column.}
#' \item{table_name}{The string name of the table name column.}
#' \item{country}{The string name of the country column.}
#' \item{method}{The string name of the method column.}
#' \item{year}{The string name of the year column.}
#' \item{last_stage}{The string name of the last_stage column.}
#' \item{energy_type}{The string name of the energy_type column.}
#' \item{tar_group}{The string name of the tar_group column.}
#' }
#'
#' @examples
#' usual_hash_group_cols
"usual_hash_group_cols"


