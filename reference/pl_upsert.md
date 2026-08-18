# Upsert a data frame with optional encoding of foreign keys

**\[superseded\]** Use
[pl_upsert_and_compress](https://energyeconomydecoupling.github.io/PFUPipelineTools/reference/pl_upsert_and_compress.md)`(compress = FALSE)`
instead.

## Usage

``` r
pl_upsert(
  .df,
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
  index_map =
    magrittr::set_names(list(fk_parent_tables[[IEATools::row_col_types$industry]],
    fk_parent_tables[[IEATools::row_col_types$product]],
    fk_parent_tables[[IEATools::row_col_types$other]]),
    c(IEATools::row_col_types$industry, IEATools::row_col_types$product,
    IEATools::row_col_types$other)),
  retain_zero_structure = FALSE,
  schema = schema_from_conn(conn),
  fk_parent_tables = get_all_fk_tables(conn = conn, schema = schema, collect = TRUE),
  .db_table_name = PFUPipelineTools::hashed_table_colnames$db_table_name,
  .pk_col = PFUPipelineTools::dm_pk_colnames$pk_col,
  .algo = "md5"
)
```

## Arguments

- .df:

  The data frame to be upserted.

- conn:

  A connection to the CL-PFU database.

- db_table_name:

  A string identifying the destination for `.df` in `conn`, i.e. the
  name of a remote database table. Default is `NULL`, meaning that the
  value for this argument will be taken from the `.db_table_name` column
  of `.df`.

- additional_hash_group_cols:

  A vector or list of additional columns by which `.df` will be grouped
  before hashing and, therefore, appear in the output. Default is
  `NULL`. Passed to
  [`pl_hash()`](https://energyeconomydecoupling.github.io/PFUPipelineTools/reference/pl_hash.md).

- usual_hash_group_cols:

  A vector of columns by which `.df` will be grouped before hashing and,
  therefore, appear in the output. Default is
  `PFUPipelineTools::additional_hash_group_cols` but can be set to
  `NULL` to disable. Passed to
  [`pl_hash()`](https://energyeconomydecoupling.github.io/PFUPipelineTools/reference/pl_hash.md).

- keep_single_unique_cols:

  A boolean that tells whether to keep columns with a single unique
  value in the output. Default is `TRUE`. Passed to
  [`pl_hash()`](https://energyeconomydecoupling.github.io/PFUPipelineTools/reference/pl_hash.md).

- in_place:

  A boolean that tells whether to modify the database at `conn`. Default
  is `FALSE`, which is helpful if you want to chain several requests.

- encode_fks:

  A boolean that tells whether to code foreign keys in `.df` before
  upserting to `conn`. Default is `TRUE`.

- compress:

  A boolean that tells whether to compress `db_table_name` in the
  database after uploading. Default is `FALSE`.

- round_double_columns:

  A boolean that tells whether to round double-precision columns in
  `.df`. Default is `FALSE`.

- digits:

  An integer that tells the number of significant digits. `digits` has
  an effect only when `round_double_columns` is `TRUE`. Default is `15`,
  which should eliminate any numerical precision errors for
  [`compress_rows()`](https://energyeconomydecoupling.github.io/PFUPipelineTools/reference/compress_rows.md).

- index_map:

  A list of 2 or more data frames that represent the mappings from
  inboard row and column indices in the database to outboard row and
  column names in the memory of the local computer. See documentation
  for
  [`encode_matsindf()`](https://energyeconomydecoupling.github.io/PFUPipelineTools/reference/encode_decode_matsindf.md)
  and
  [`matsbyname::to_triplet()`](https://matthewheun.github.io/matsbyname/reference/to_named_triplet.html).
  Default is a `list` that contains the `industry`, `product`, and
  `other` members of `fk_parent_tables`.

- retain_zero_structure:

  A boolean that tells whether to retain the structure of zero matrices.
  See details.

- schema:

  The data model (`dm` object) for the database in `conn`. Default is
  `dm_from_con(conn, learn_keys = TRUE)`. See details.

- fk_parent_tables:

  A named list of all parent tables for the foreign keys in
  `db_table_name`. See details.

- .db_table_name:

  The name of the table name column in `.df`. Default is
  `PFUPipelineTools::hashed_table_colnames$db_table_name`.

- .pk_col:

  The name of the primary key column in a primary key table. See
  [`PFUPipelineTools::dm_pk_colnames`](https://energyeconomydecoupling.github.io/PFUPipelineTools/reference/dm_pk_colnames.md).

- .algo:

  The hashing algorithm. Default is "md5".

## Value

A hash of `.df` according to `.algo`.

## Details

Upserts (inserts or updates, depending on whether the private keys in
`.df` already exist in `db_table_name`) `.df` into `db_table_name` at
`conn`.

This function decodes foreign keys (fks), when possible, assuming that
all fks are integers. If non-integers (typically, character strings) are
provided in fk columns of `.df`, the non-integers will be recoded to
their appropriate integer key values.

This function knows about CL-PFU database tables that contain matrix
information. In particular, if `.df` contains matrices, they are
expanded into row-col-val format before uploading.

The output of this function is a special data frame that contains the
following columns:

- All single-valued columns columns in `.df`, columns given in
  `additional_hash_group_cols` (default `NULL`), and columns given in
  `usual_hash_group_cols` (default
  [`PFUPipelineTools::usual_hash_group_cols`](https://energyeconomydecoupling.github.io/PFUPipelineTools/reference/usual_hash_group_cols.md)).

- Hash: A column with a hash of all non-foreign-key columns.

`schema` is a data model (`dm` object) for the database in `conn`. Its
default value (`schema_from_conn(conn)`) extracts the data model for the
database at `conn` automatically. However, if the caller already has the
data model, supplying it in the `schema` argument will save time.

`fk_parent_tables` is a named list of tables, one of which (the one
named `db_table_name`) contains the foreign keys for `db_table_name`.
`fk_parent_tables` is treated as a store from which foreign key tables
are retrieved by name when needed. The default value (which calls
[`get_all_fk_tables()`](https://energyeconomydecoupling.github.io/PFUPipelineTools/reference/get_all_fk_tables.md)
with `collect = TRUE` because decoding of foreign keys is done outboard
of the database) retrieves all possible foreign key parent tables from
`conn`, potentially a time-consuming process. For speed, pre-compute all
foreign key parent tables once (via `get_all_fk_tables(collect = TRUE)`)
and pass the list to the `fk_parent_tables` argument of this function.

The user in `conn` must have write access to the database.

By default, `pl_upsert()` will delete all zero entries in matrices
before upserting. But for some countries and years, that could result in
missing matrices, such as **U_EIOU**. Set `retain_zero_structure = TRUE`
to preserve all entries in a zero matrix.

## See also

`pl_download()` for the reverse operation.
[`pl_upload_schema_and_simple_tables()`](https://energyeconomydecoupling.github.io/PFUPipelineTools/reference/pl_upload_schema_and_simple_tables.md)
for a way to establish the database schema.
