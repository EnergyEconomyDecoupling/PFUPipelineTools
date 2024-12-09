---
title: "Release notes for `PFUPipelineTools`"
output: html_document
---


Cite all releases with doi [10.5281/zenodo.8226419](https://doi.org/10.5281/zenodo.8226419), 
which always resolves to the latest release.




* Updates to many accessing functions for the database.


# PFUPipelineTools 0.1.10 (2024-08-01)

* Eliminated the `IEAMW` column everywhere.
  The `Dataset` column is now doing the work of the `IEAMW` column.


# PFUPipelineTools 0.1.9 (2024-07-30)

* Added new "date" option for foreign key columns.
* Now using "value" column instead of "x".
* Now allowing foreign key tables to contain more than 
  only the ID column and the foreign key column.
  This is particularly useful for, e.g., 
  the energy type table:
  EnergyTypeID	EnergyType	FullName	Description
  1             E           Energy    Energy is a thermal quantification of energy.
  2             X	          Exergy    Exergy is a work quantification of energy.
* New functions for interacting with databases, including
    - `pl_filter_collect()`
    - `load_schema_table()`
    - `load_fk_tables()`
    - `schema_dm()`
    - `pl_upload_schema_and_simple_tables()`
    - `set_not_null_constraints_on_fk_cols()`
    - `pl_upsert()`
    - `encode_fks()`
    - `decode_fks()`
    - `encode_fk_values()`
    - `encode_fk_keys()`
* Removed `stash_cache()` and associated tests.
  We no longer save the pipeline cache.
  It seemed like a good idea at the time, but
  we never looked at saved caches.
* New function `self_name()` is helpful in many places.
* Several new functions to assist with database schema, etc.


# PFUPipelineTools 0.1.8 (2023-12-21) [![DOI](https://zenodo.org/badge/DOI/10.5281/zenodo.10420449.svg)](https://doi.org/10.5281/zenodo.10420449)

* Added a statement of need to `README.Rmd`.
* No new tests
    - Still at 24 tests, all passing.
    - Test coverage remains at 100%.


# PFUPipelineTools 0.1.7 (2023-12-08) [![DOI](https://zenodo.org/badge/DOI/10.5281/zenodo.10308793.svg)](https://doi.org/10.5281/zenodo.10308793)

* Added package dependencies for test coverage workflow.
* No new tests
    - Still at 24 tests, all passing.
    - Test coverage remains at 100%.


# PFUPipelineTools 0.1.6 (2023-12-04) [![DOI](https://zenodo.org/badge/DOI/10.5281/zenodo.10256768.svg)](https://doi.org/10.5281/zenodo.10256768)

* Add the package dependencies to the code coverage GitHub action.
* No new tests
    - Still at 24 tests, all passing.
    - Test coverage remains at 100%.


# PFUPipelineTools 0.1.5 (2023-12-04) [![DOI](https://zenodo.org/badge/DOI/10.5281/zenodo.10256712.svg)](https://doi.org/10.5281/zenodo.10256712)

* Add the develop and release-* branches to GitHub actions
  for R-CMD-CHECK.
* No new tests
    - Still at 24 tests, all passing.
    - Test coverage remains at 100%.


# PFUPipelineTools 0.1.4 (2023-12-03)

* Attempting to fix a bug in the continuous integration process.
  Builds are failing due to missing external dependencies.
* No new tests
    - Still at 24 tests, all passing.
    - Test coverage remains at 100%.


# PFUPipelineTools 0.1.3 (2023-12-03) [![DOI](https://zenodo.org/badge/DOI/10.5281/zenodo.10253202.svg)](https://doi.org/10.5281/zenodo.10253202)

* New function `read_pin_version()`
* Added GitHub actions for generating website.
* Beginning code coverage support.
* No new tests
    - Still at 24 tests, all passing.
    - Test coverage remains at 100%.


# PFUPipelineTools 0.1.2 (2023-08-08) [![DOI](https://zenodo.org/badge/DOI/10.5281/zenodo.8226420.svg)](https://doi.org/10.5281/zenodo.8226420)

* First release to be assigned a Zenodo DOI.
* Added several new tests
  to get to 100% coverage.


# PFUPipelineTools 0.1.1 (2023-06-06)

* Initial release
* Added a `NEWS.md` file to track changes to the package.
* Added first tests.
    * Only 2 tests, both passing.
    * Test coverage is low (17 %) but will improve.
