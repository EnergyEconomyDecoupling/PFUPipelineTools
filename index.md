# PFUPipelineTools

## Statement of need

Several packages are involved in creating the CL-PFU database, and many
of those packages provide computational pipelines for the database.
Those pipelines need access to common functions, say for releasing
(saving) computational targets to their respective pins, for reading
targets from their pins, for filtering by countries and years, or
removing target groupings.

This package (`PFUPipelineTools`) addresses that need, providing a
single package for several functions that are used across various CL-PFU
database pipelines.

## Installation

You can install the development version of `PFUPipelineTools` from
[GitHub](https://github.com/) with:

``` r

# install.packages("devtools")
devtools::install_github("EnergyEconomyDecoupling/PFUPipelineTools")
```

## More Information

Find more information, including vignettes and function documentation,
at <https://EnergyEconomyDecoupling.github.io/PFUPipelineTools/>.
