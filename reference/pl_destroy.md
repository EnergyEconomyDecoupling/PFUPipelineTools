# Reset the CL-PFU database pipeline to original condition

Sometimes, you just need to start over from scratch. This function
destroys the local `targets` cache and removes all tables in the
database at `conn`. Be sure you know what you're doing!

## Usage

``` r
pl_destroy(
  conn,
  store = targets::tar_config_get("store"),
  destroy_cache = FALSE,
  drop_tables = FALSE
)
```

## Arguments

- conn:

  A `DBI` connection to a database.

- store:

  The path to the `targets` store. Default is
  `targets::tar_config_get("store")`, which is normally `_targets`.

- destroy_cache:

  A boolean that tells whether to destroy the local `targets` cache.
  Default is `FALSE`.

- drop_tables:

  If `TRUE`, all tables in conn are dropped. If a character vector,
  tells which tables to drop. Default is `FALSE`, meaning that no tables
  will be dropped.

## Value

The names of tables dropped (if any) or an empty character vector when
no tables are dropped.

## Details

`conn`'s user must have superuser privileges.

Because this is a very destructive function, the caller must opt into
behaviors.

When `drop_tables` is `TRUE`, `destroy_cache` is implied to be `TRUE`,
and the targets cache is destroyed.

At present
