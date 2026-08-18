# Read a version of a pinned CL-PFU database product

Read a version of a pinned CL-PFU database product

## Usage

``` r
read_pin_version(
  pin_name,
  database_version,
  pin_version_string = PFUSetup::pin_versions(database_version)[[pin_name]],
  pipeline_releases_folder = PFUSetup::get_abs_paths()[["pipeline_releases_folder"]]
)
```

## Arguments

- pin_name:

  The string name of the pin to be read.

- database_version:

  A string, prefixed with "v" for the version of interest. Any number
  will be prefixed by "v" and converted to a string internally.

- pin_version_string:

  The version string for pin `pin_name` associated with
  `database_version`. Default is
  `pin_versions(database_version)[[pin_name]]`.

- pipeline_releases_folder:

  The path to the pipeline releases folder. Default is
  `get_abs_paths()[["pipeline_releases_folder"]]`.

## Value

The pinned object represented by the name and the version string.

## Examples

``` r
if (FALSE) { # \dontrun{
read_pin_version(pin_name = "phi_vecs", database_version = 1.2) |>
  head()
} # }
```
