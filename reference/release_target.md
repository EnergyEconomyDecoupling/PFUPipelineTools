# Save a single target to a pinboard.

Releases (`release = TRUE`) or not (`release = FALSE`) a new version of
the single target `targ` using the `pins` package.

## Usage

``` r
release_target(
  pipeline_releases_folder,
  targ,
  pin_name,
  type = "rds",
  release = FALSE
)
```

## Arguments

- pipeline_releases_folder:

  The folder that contains the pinboard for releases from the pipeline.

- targ:

  The target R object to be saved to the pinboard.

- pin_name:

  The name of the pin in the pinboard. `pin_name` is the key to
  retrieving `targ`.

- type:

  The type of the target, routed to
  [`pins::pin_write()`](https://pins.rstudio.com/reference/pin_read.html).
  Default is "rds". "csv" in another option.

- release:

  A boolean telling whether to do a release. Default is `FALSE`.

## Value

If `release` is `TRUE`, the fully-qualified path name of the `targ` file
in the pinboard. If `release` is `FALSE`, the string "Release not
requested."

## Details

Released versions of the target can be obtained as shown in examples.

## Examples

``` r
if (FALSE) { # \dontrun{
# Establish the pinboard
pinboard <- file.path("~",
                      "Dropbox",
                      "Fellowship 1960-2015 PFU database",
                      "OutputData", "PipelineReleases") |>
  pins::board_folder()
# Get information about the `PSUT` target in the pinboard
pinboard |>
  pins::pin_meta(name = "psut")
# Find versions of the `PSUT` target
pinboard |>
  pins::pin_versions(name = "psut")
# Get the latest copy of the `PSUT` target.
my_psut <- pinboard |>
  pins::pin_read(name = "psut")
# Retrieve a previous version of the `PSUT` target.
my_old_psut <- pinboard |>
  pins::pin_read(name = "psut", version = "20220218T023112Z-1d9e1")
} # }
```
