# Calculate hash of pipeline data

In the CL-PFU database pipeline, we need the ability to download a data
frame from the database based on a hash of the data. This function
calculates the appropriate hash.

## Usage

``` r
pl_hash(
  .df = NULL,
  table_name,
  conn,
  usual_hash_group_cols = PFUPipelineTools::usual_hash_group_cols,
  additional_hash_group_cols = NULL,
  keep_single_unique_cols = TRUE,
  .table_name_col = PFUPipelineTools::hashed_table_colnames$db_table_name,
  .nested_hash_col = PFUPipelineTools::hashed_table_colnames$nested_hash_colname,
  .nested_col = PFUPipelineTools::hashed_table_colnames$nested_colname,
  tar_group_col = "tar_group",
  .algo = "md5"
)
```

## Arguments

- .df:

  An in-memory data frame to be stored in the database or `NULL` if the
  hash of a table in the database at `conn` is desired.

- table_name:

  The string name of the table in which `.df` will be stored or the name
  of a table in the database to be hashed.

- conn:

  A connection to a database. Necessary only if `.df` is `NULL` (its
  default value).

- usual_hash_group_cols:

  The string vector of usual column names to be preserved in the hashed
  data frame. Default is
  [`PFUPipelineTools::usual_hash_group_cols`](https://energyeconomydecoupling.github.io/PFUPipelineTools/reference/usual_hash_group_cols.md).

- additional_hash_group_cols:

  A string vector of names of additional columns by which `.df` will be
  grouped before making the `.nested_hash_col` hash column. All
  `additional_hash_group_cols` that exist in the data frame or table
  being hashed will be present in the result. Default is `NULL`, meaning
  that grouping will be done on all columns that contain only 1 unique
  value. See details.

- keep_single_unique_cols:

  A boolean that tells whether to keep columns that have a single unique
  value in the outgoing hash. Default is `TRUE`.

- .table_name_col:

  The name of the column of the output that contains `table_name`.
  Default is `PFUPipelineTools::hashed_table_colnames$db_table_name`.

- .nested_hash_col:

  The name of the column of the output that contains the hash of nested
  columns. Default is
  `PFUPipelineTools::hashed_table_colnames$nested_hash_colname`.

- .nested_col:

  The name of the column of the output that contains nested data. Used
  internally. Default is
  `PFUPipelineTools::hashed_table_colnames$nested_colname`.

- tar_group_col:

  The name of the tar_group column. Default is "tar_group".

- .algo:

  The algorithm for hashing. Default is "md5".

## Value

A data frame "ticket" for later retrieving data from the database.

## Details

The hash has two requirements:

- values change when content changes and

- provides sufficient information to retrieve the data frame from the
  database.

The return value from this function (being a special hash of a database
table) serves as a "ticket" with which data can be retrieved from the
database at a later time using
[`pl_collect_from_hash()`](https://energyeconomydecoupling.github.io/PFUPipelineTools/reference/pl_collect_from_hash.md).

To meet the requirements of the hash, the return value from this
function has the following characteristics:

- The first column (named with the value of `.table_name_col`) contains
  the value of `table_name`, the name of the database table where the
  actual data frame is stored.

- The last column (at the right and named with the value of
  `.nested_col`) contains a hash of a data frame created by nesting by
  all columns with more than one unique value and
  `additional_hash_group_cols` (when `additional_hash_group_cols` is not
  `NULL`).

- The second through N-1 columns (inclusive) are all columns with only
  one unique value (provided that `keep_single_unique_cols` is `TRUE`
  AND those columns specified by `additional_hash_group_cols` (provided
  that `additional_hash_group_cols` is not `NULL`, the default).

If `keep_single_unique_cols` is `FALSE` and `additional_hash_group_cols`
is `NULL`, an error is raised.

Hashes can be created from data frames in memory, typically about to be
uploaded to the database. To do so, supply `.df` as a data frame. If the
`.table_name_column` is not present in `.df`, it is added internally,
filled with the value of `table_name`.

Alternatively, hashes can be created from a table already existing in
the database at `conn`. To do this, leave `.df` at its default value
(`NULL`) and supply the `table_name` and `conn` arguments. In this case,
an SQL query is generated and a hash of the entire table is provided as
the return value. `.table_name_column` is added to the result after
downloading.

Both approaches use the `md5` hashing algorithm.

That said, the two approaches do not give the same hashes for the same
data frame, due to differences in the way that the database creates its
hash vs. how R creates its hash.
