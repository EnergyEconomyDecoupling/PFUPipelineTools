# Changelog

## PFUPipelineTools 0.1.15 (2026-08-18)

- Fixed a bug in the examples for
  [`schema_dm()`](https://energyeconomydecoupling.github.io/PFUPipelineTools/reference/schema_dm.md).
- Fixed a bug where
  [`pl_collect_from_hash()`](https://energyeconomydecoupling.github.io/PFUPipelineTools/reference/pl_collect_from_hash.md)
  did not replace values in the `ValidFromVersion` and `ValidToVersion`
  columns with the requested version. The solution was implemented at a
  low level in
  [`filter_on_version_string()`](https://energyeconomydecoupling.github.io/PFUPipelineTools/reference/filter_on_version_string.md)
  and applies to both
  [`pl_collect_from_hash()`](https://energyeconomydecoupling.github.io/PFUPipelineTools/reference/pl_collect_from_hash.md)
  and
  [`pl_filter_collect()`](https://energyeconomydecoupling.github.io/PFUPipelineTools/reference/pl_filter_collect.md),
  meaning that similar code could be removed from
  [`pl_filter_collect()`](https://energyeconomydecoupling.github.io/PFUPipelineTools/reference/pl_filter_collect.md).
- Fixed a bug where updating the `ValidToVersion` column for an updated
  row failed because the `by` argument had too few column names.
- No longer removing the `SchemaTable` table from the list of tables to
  be uploaded in
  [`load_fk_tables()`](https://energyeconomydecoupling.github.io/PFUPipelineTools/reference/load_fk_tables.md).
  This means that the database will contain its own schema in the
  `SchemaTable` table.
- Improvements to `pl_filter_download()`, including new options to
  control whether encoded or decoded data are downloaded.
- New functions
  [`pl_upsert_and_compress()`](https://energyeconomydecoupling.github.io/PFUPipelineTools/reference/pl_upsert_and_compress.md)
  and
  [`compress_helper()`](https://energyeconomydecoupling.github.io/PFUPipelineTools/reference/compress_helper.md)
  allow multiple value columns in a data frame and remote table.
- New function
  [`pl_upsert_and_compress()`](https://energyeconomydecoupling.github.io/PFUPipelineTools/reference/pl_upsert_and_compress.md)
  takes over duties from
  [`pl_upsert()`](https://energyeconomydecoupling.github.io/PFUPipelineTools/reference/pl_upsert.md).
  [`pl_upsert_and_compress()`](https://energyeconomydecoupling.github.io/PFUPipelineTools/reference/pl_upsert_and_compress.md)
  performs local compression, deciding how and when to compress database
  tables using the `ValidFromVersion` and `ValidToVersion` columns.
  [`pl_upsert_and_compress()`](https://energyeconomydecoupling.github.io/PFUPipelineTools/reference/pl_upsert_and_compress.md)
  compresses by default and allows more options.
  [`pl_upsert()`](https://energyeconomydecoupling.github.io/PFUPipelineTools/reference/pl_upsert.md)
  is deprecated.
- New function
  [`compress_helper()`](https://energyeconomydecoupling.github.io/PFUPipelineTools/reference/compress_helper.md)
  encapsulates the logic for deciding which rows to change when
  uploading to the database.
- New function
  [`get_unit_testing_conn()`](https://energyeconomydecoupling.github.io/PFUPipelineTools/reference/db-connections.md)
  provides convenience during unit testing.
- New tests for new features.
  - Now up to 373 tests, all passing
  - Test coverage now reported to be 45.78%, but that’s likely an
    undercount. Many functions are not tested on continuous integration
    platforms and CRAN.

## PFUPipelineTools 0.1.14 (2025-07-22) [![DOI](https://zenodo.org/badge/DOI/10.5281/zenodo.16323795.svg)](https://doi.org/10.5281/zenodo.16323795)

- New `get_*db_conn()` functions assist creating database connections to
  <https://mexer.site>.
- [`update_schema_table()`](https://energyeconomydecoupling.github.io/PFUPipelineTools/reference/update_schema_table.md)
  is now an exported function.
- New tests for new features.
  - Now up to 238 tests, all passing
  - Test coverage now reported to be 24.64%, but that’s an undercount.
    Many (most?) functions are not tested on continuous integration
    platforms and CRAN.

## PFUPipelineTools 0.1.13 (2025-05-15) [![DOI](https://zenodo.org/badge/DOI/10.5281/zenodo.15427749.svg)](https://doi.org/10.5281/zenodo.15427749)

- [`pl_upsert()`](https://energyeconomydecoupling.github.io/PFUPipelineTools/reference/pl_upsert.md)
  gains new arguments `round_double_columns` and `digits` and calls
  [`round_double_cols()`](https://energyeconomydecoupling.github.io/PFUPipelineTools/reference/round_double_cols.md)
  if requested.
- [`round_double_cols()`](https://energyeconomydecoupling.github.io/PFUPipelineTools/reference/round_double_cols.md)
  rounds double-precision columns in a data frame to assist the
  `compress()` function in the database.
- [`pl_upsert()`](https://energyeconomydecoupling.github.io/PFUPipelineTools/reference/pl_upsert.md)
  gains new argument `compress`, which defaults to `FALSE`. If set to
  `TRUE`,
  [`compress_rows()`](https://energyeconomydecoupling.github.io/PFUPipelineTools/reference/compress_rows.md)
  is called internally within
  [`pl_upsert()`](https://energyeconomydecoupling.github.io/PFUPipelineTools/reference/pl_upsert.md).
- New functions
  [`install_compress_function()`](https://energyeconomydecoupling.github.io/PFUPipelineTools/reference/install_compress_function.md),
  [`remove_compress_function()`](https://energyeconomydecoupling.github.io/PFUPipelineTools/reference/remove_compress_function.md),
  and
  [`compress_rows()`](https://energyeconomydecoupling.github.io/PFUPipelineTools/reference/compress_rows.md)
  assist with compressing rows in the remote database.
- [`pl_filter_collect()`](https://energyeconomydecoupling.github.io/PFUPipelineTools/reference/pl_filter_collect.md)
  gains new argument `...` in which filtering expressions can be placed.
  `...` replaces the myriad other arguments that provided filtering for
  possible columns. This is a breaking change, but the new approach
  provides significant flexibility for users of this function. Plus, the
  code is much cleaner inside
  [`pl_filter_collect()`](https://energyeconomydecoupling.github.io/PFUPipelineTools/reference/pl_filter_collect.md)!
- In several places, code no longer passes `conn` when both `schema` and
  `fk_parent_tables` are known.
- Added a safety check in
  [`pl_destroy()`](https://energyeconomydecoupling.github.io/PFUPipelineTools/reference/pl_destroy.md)
  to disable destroying “MexerDB”.
- [`pl_collect_from_hash()`](https://energyeconomydecoupling.github.io/PFUPipelineTools/reference/pl_collect_from_hash.md)
  and
  [`pl_filter_collect()`](https://energyeconomydecoupling.github.io/PFUPipelineTools/reference/pl_filter_collect.md)
  gain new argument `version_string` that provides capability to filter
  downloads by version. The default value (`NULL`) downloads all
  versions. Multiple versions can be downloaded by passing a vector of
  strings. Supplying [`c()`](https://rdrr.io/r/base/c.html) (an empty
  vector) downloads a table with no rows.
- [`pl_collect_from_hash()`](https://energyeconomydecoupling.github.io/PFUPipelineTools/reference/pl_collect_from_hash.md)
  is now more convenient with default arguments for `schema` and
  `fk_parent_tables` that pull values from `conn`. This new behavior for
  [`pl_collect_from_hash()`](https://energyeconomydecoupling.github.io/PFUPipelineTools/reference/pl_collect_from_hash.md)
  is now consistent with
  [`pl_filter_collect()`](https://energyeconomydecoupling.github.io/PFUPipelineTools/reference/pl_filter_collect.md).
- New tests for new features.
  - Now up to 233 tests, all passing
  - Test coverage now reported to be 25.27%, but that’s an undercount.
    Many (most?) functions are not tested on continuous integration
    platforms and CRAN.

## PFUPipelineTools 0.1.12 (2024-12-09) [![DOI](https://zenodo.org/badge/DOI/10.5281/zenodo.14589472.svg)](https://doi.org/10.5281/zenodo.14589472)

- Improved defaults for arguments to
  [`pl_filter_collect()`](https://energyeconomydecoupling.github.io/PFUPipelineTools/reference/pl_filter_collect.md).
- No new tests.
  - Still at 143 tests, all passing
  - Test coverage now reported to be 27.37%

## PFUPipelineTools 0.1.11 (2024-12-09)

- Updates to many accessing functions for the database.
- Added several new tests for new accessing functions.
  - Now up to 143 tests, all passing
  - Test coverage now at 92.73%

## PFUPipelineTools 0.1.10 (2024-08-01)

- Eliminated the `IEAMW` column everywhere. The `Dataset` column is now
  doing the work of the `IEAMW` column.

## PFUPipelineTools 0.1.9 (2024-07-30)

- Added new “date” option for foreign key columns.
- Now using “value” column instead of “x”.
- Now allowing foreign key tables to contain more than only the ID
  column and the foreign key column. This is particularly useful for,
  e.g., the energy type table: EnergyTypeID EnergyType FullName
  Description 1 E Energy Energy is a thermal quantification of energy. 2
  X Exergy Exergy is a work quantification of energy.
- New functions for interacting with databases, including
  - [`pl_filter_collect()`](https://energyeconomydecoupling.github.io/PFUPipelineTools/reference/pl_filter_collect.md)
  - [`load_schema_table()`](https://energyeconomydecoupling.github.io/PFUPipelineTools/reference/load_schema_table.md)
  - [`load_fk_tables()`](https://energyeconomydecoupling.github.io/PFUPipelineTools/reference/load_fk_tables.md)
  - [`schema_dm()`](https://energyeconomydecoupling.github.io/PFUPipelineTools/reference/schema_dm.md)
  - [`pl_upload_schema_and_simple_tables()`](https://energyeconomydecoupling.github.io/PFUPipelineTools/reference/pl_upload_schema_and_simple_tables.md)
  - [`set_not_null_constraints_on_fk_cols()`](https://energyeconomydecoupling.github.io/PFUPipelineTools/reference/set_not_null_constraints_on_fk_cols.md)
  - [`pl_upsert()`](https://energyeconomydecoupling.github.io/PFUPipelineTools/reference/pl_upsert.md)
  - [`encode_fks()`](https://energyeconomydecoupling.github.io/PFUPipelineTools/reference/encode_fks.md)
  - [`decode_fks()`](https://energyeconomydecoupling.github.io/PFUPipelineTools/reference/decode_fks.md)
  - [`encode_fk_values()`](https://energyeconomydecoupling.github.io/PFUPipelineTools/reference/encode_fk_values.md)
  - `encode_fk_keys()`
- Removed `stash_cache()` and associated tests. We no longer save the
  pipeline cache. It seemed like a good idea at the time, but we never
  looked at saved caches.
- New function
  [`self_name()`](https://energyeconomydecoupling.github.io/PFUPipelineTools/reference/self_name.md)
  is helpful in many places.
- Several new functions to assist with database schema, etc.

## PFUPipelineTools 0.1.8 (2023-12-21) [![DOI](https://zenodo.org/badge/DOI/10.5281/zenodo.10420449.svg)](https://doi.org/10.5281/zenodo.10420449)

- Added a statement of need to `README.Rmd`.
- No new tests
  - Still at 24 tests, all passing.
  - Test coverage remains at 100%.

## PFUPipelineTools 0.1.7 (2023-12-08) [![DOI](https://zenodo.org/badge/DOI/10.5281/zenodo.10308793.svg)](https://doi.org/10.5281/zenodo.10308793)

- Added package dependencies for test coverage workflow.
- No new tests
  - Still at 24 tests, all passing.
  - Test coverage remains at 100%.

## PFUPipelineTools 0.1.6 (2023-12-04) [![DOI](https://zenodo.org/badge/DOI/10.5281/zenodo.10256768.svg)](https://doi.org/10.5281/zenodo.10256768)

- Add the package dependencies to the code coverage GitHub action.
- No new tests
  - Still at 24 tests, all passing.
  - Test coverage remains at 100%.

## PFUPipelineTools 0.1.5 (2023-12-04) [![DOI](https://zenodo.org/badge/DOI/10.5281/zenodo.10256712.svg)](https://doi.org/10.5281/zenodo.10256712)

- Add the develop and release-\* branches to GitHub actions for
  R-CMD-CHECK.
- No new tests
  - Still at 24 tests, all passing.
  - Test coverage remains at 100%.

## PFUPipelineTools 0.1.4 (2023-12-03)

- Attempting to fix a bug in the continuous integration process. Builds
  are failing due to missing external dependencies.
- No new tests
  - Still at 24 tests, all passing.
  - Test coverage remains at 100%.

## PFUPipelineTools 0.1.3 (2023-12-03) [![DOI](https://zenodo.org/badge/DOI/10.5281/zenodo.10253202.svg)](https://doi.org/10.5281/zenodo.10253202)

- New function
  [`read_pin_version()`](https://energyeconomydecoupling.github.io/PFUPipelineTools/reference/read_pin_version.md)
- Added GitHub actions for generating website.
- Beginning code coverage support.
- No new tests
  - Still at 24 tests, all passing.
  - Test coverage remains at 100%.

## PFUPipelineTools 0.1.2 (2023-08-08) [![DOI](https://zenodo.org/badge/DOI/10.5281/zenodo.8226420.svg)](https://doi.org/10.5281/zenodo.8226420)

- First release to be assigned a Zenodo DOI.
- Added several new tests to get to 100% coverage.

## PFUPipelineTools 0.1.1 (2023-06-06)

- Initial release
- Added a `NEWS.md` file to track changes to the package.
- Added first tests.
  - Only 2 tests, both passing.
  - Test coverage is low (17 %) but will improve.
