# This script creates constants and saves them in the right locations.
# If there are any changes to these constants,
# source this script before building the package.

library(IEATools)

#
# All countries to run in the workflow
#

all_countries <- list(
  afri = "AFRI",
  ago = "AGO",
  alb = "ALB",
  are = "ARE",
  arg = "ARG",
  arm = "ARM",
  asia = "ASIA",
  aus = "AUS",
  aut = "AUT",
  aze = "AZE",
  bel = "BEL",
  ben = "BEN",
  bgd = "BGD",
  bgr = "BGR",
  bhr = "BHR",
  bih = "BIH",
  blr = "BLR",
  bol = "BOL",
  bra = "BRA",
  brn = "BRN",
  bwa = "BWA",
  bunk = "BUNK",
  can = "CAN",
  che = "CHE",
  chl = "CHL",
  chnm = "CHNM",
  cmr = "CMR",
  cod = "COD",
  cog = "COG",
  col = "COL",
  civ = "CIV",
  cri = "CRI",
  cub = "CUB",
  cuw = "CUW",
  cyp = "CYP",
  cze = "CZE",
  deu = "DEU",
  dnk = "DNK",
  dom = "DOM",
  dza = "DZA",
  ecu = "ECU",
  egy = "EGY",
  eri = "ERI",
  esp = "ESP",
  est = "EST",
  eth = "ETH",
  eurp = "EURP",
  fin = "FIN",
  fra = "FRA",
  gab = "GAB",
  gbr = "GBR",
  geo = "GEO",
  gha = "GHA",
  gib = "GIB",
  gnq = "GNQ",
  grc = "GRC",
  gtm = "GTM",
  guy = "GUY",
  hkg = "HKG",
  hnd = "HND",
  hrv = "HRV",
  hti = "HTI",
  hun = "HUN",
  idn = "IDN",
  ind = "IND",
  irl = "IRL",
  irn = "IRN",
  irq = "IRQ",
  isl = "ISL",
  isr = "ISR",
  ita = "ITA",
  jam = "JAM",
  jor = "JOR",
  jpn = "JPN",
  kaz = "KAZ",
  ken = "KEN",
  kgz = "KGZ",
  khm = "KHM",
  kor = "KOR",
  kwt = "KWT",
  lao = "LAO",
  lbn = "LBN",
  lby = "LBY",
  lka = "LKA",
  ltu = "LTU",
  lux = "LUX",
  lva = "LVA",
  mar = "MAR",
  mda = "MDA",
  mdg = "MDG",
  mex = "MEX",
  mide = "MIDE",
  mkd = "MKD",
  mlt = "MLT",
  mmr = "MMR",
  mne = "MNE",
  mng = "MNG",
  moz = "MOZ",
  mus = "MUS",
  mys = "MYS",
  nam = "NAM",
  namr = "NAMR",
  ner = "NER",
  nga = "NGA",
  nic = "NIC",
  nld = "NLD",
  nor = "NOR",
  npl = "NPL",
  nzl = "NZL",
  oafr = "OAFR",
  oasi = "OASI",
  oamr = "OAMR",
  ocen = "OCEN",
  omn = "OMN",
  pak = "PAK",
  pan = "PAN",
  per = "PER",
  phl = "PHL",
  pol = "POL",
  prk = "PRK",
  prt = "PRT",
  pry = "PRY",
  qat = "QAT",
  rou = "ROU",
  rus = "RUS",
  rwa = "RWA",
  samr = "SAMR",
  sau = "SAU",
  sdn = "SDN",
  sen = "SEN",
  sgp = "SGP",
  slv = "SLV",
  srb = "SRB",
  ssd = "SSD",
  sun = "SUN",
  sur = "SUR",
  svk = "SVK",
  svn = "SVN",
  swe = "SWE",
  swz = "SWZ",
  syr = "SYR",
  tgo = "TGO",
  tha = "THA",
  tjk = "TJK",
  tkm = "TKM",
  tto = "TTO",
  tun = "TUN",
  tur = "TUR",
  twn = "TWN",
  tza = "TZA",
  uga = "UGA",
  ukr = "UKR",
  ury = "URY",
  usa = "USA",
  uzb = "UZB",
  ven = "VEN",
  vnm = "VNM",
  wabk = "WABK",
  wrld = "WRLD",
  wmbk = "WMBK",
  xkx = "XKX",
  yem = "YEM",
  yug = "YUG",
  zaf = "ZAF",
  zmb = "ZMB",
  zwe = "ZWE"
)

usethis::use_data(all_countries, overwrite = TRUE)


#
# Countries whose data also exists in another 'country';
# e.g., Memo: Uganda (UGA)
# in Other Africa (OAF).
#

double_counted_countries <- list(
  afri = "AFRI",
  asia = "ASIA",
  bunk = "BUNK",
  eurp = "EURP",
  mide = "MIDE",
  namr = "NAMR",
  ocen = "OCEN",
  samr = "SAMR",
  wrld = "WRLD"
)

usethis::use_data(double_counted_countries, overwrite = TRUE)


#
# Countries to run in the workflow which should sum to World (WRLD)
#

canonical_countries <- dplyr::setdiff(all_countries,
                                      double_counted_countries)

usethis::use_data(canonical_countries, overwrite = TRUE)


#
# Metadata about table keys
#

key_col_info <- list(
  pk_suffix = "ID"
)

usethis::use_data(key_col_info, overwrite = TRUE)


#
# schema_table_colnames
#

schema_table_colnames <- list(
  table = "Table",
  colname = "Colname",
  is_pk = "IsPK",
  coldatatype = "ColDataType",
  fk_table = "FKTable",
  fk_colname = "FKColname"
)

usethis::use_data(schema_table_colnames, overwrite = TRUE)


#
# Column names in primary key data frames from the `dm` package
#

dm_pk_colnames <- list(
  table = "table",
  pk_col = "pk_col",
  autoincrement = "autoincrement"
)

usethis::use_data(dm_pk_colnames, overwrite = TRUE)


#
# Column names in foreign key data frames from the `dm` package
#

dm_fk_colnames <- list(
  child_table = "child_table",
  child_fk_cols = "child_fk_cols",
  parent_table = "parent_table",
  parent_key_cols = "parent_key_cols",
  on_delete = "on_delete"
)

usethis::use_data(dm_fk_colnames, overwrite = TRUE)


#
# Hashed table colnames
#

hashed_table_colnames <- list(
  db_table_name = "DBTableName",
  nested_colname = "NestedData",
  nested_hash_colname = "NestedDataHash",
  tar_group_colname = "tar_group"
)

usethis::use_data(hashed_table_colnames, overwrite = TRUE)


#
# Beatles tables
#

beatles_file_path <- file.path("data-raw", "BeatlesSchema.xlsx")
beatles_schema_table <- load_schema_table(schema_path = beatles_file_path,
                                          schema_sheet = "Schema")

usethis::use_data(beatles_schema_table, overwrite = TRUE)

beatles_fk_tables <- load_fk_tables(simple_tables_path = beatles_file_path,
                                        readme_sheet = "README",
                                        schema_sheet = "Schema")

usethis::use_data(beatles_fk_tables, overwrite = TRUE)


#
# Tab name for machine efficiencies
#

machine_constants <- list(efficiency_tab_name = "FIN_ETA")
usethis::use_data(machine_constants, overwrite = TRUE)


#
# Column names in PSUT data frames
#

# ieamw_cols <- list(#ieamw = "IEAMW",
#                    ieamw = "Dataset",
#                    iea = "CL-PFU IEA",
#                    mw = "CL-PFU MW",
#                    both = "CL-PFU IEA+MW")
# usethis::use_data(ieamw_cols, overwrite = TRUE)


#
# Names and constants associated with exemplar tables.
#

exemplar_names <- list(exemplar_tab_name = "exemplar_table",
                       prev_names = "PrevNames",
                       exemplars = "Exemplars",
                       exemplar_country = "ExemplarCountry",
                       exemplar_countries = "ExemplarCountries",
                       exemplar_tables = "ExemplarTables",
                       iea_data = "IEAData",
                       alloc_data = "AllocData",
                       incomplete_alloc_table = "IncompleteAllocTable",
                       complete_alloc_table = "CompleteAllocTable",
                       incomplete_eta_table = "IncompleteEtaTable",
                       complete_eta_table = "CompleteEtaTable",
                       region_code = "RegionCode",
                       country_name = "CountryName",
                       agg_code_col = "AggCode",
                       world = "WRLD")
usethis::use_data(exemplar_names, overwrite = TRUE)


#
# phi.sources
#

phi_sources <- list(eta_fu_tables = "etafuTables",
                    temperature_data = "TemperatureData",
                    phi_constants = "phiConstants")
usethis::use_data(phi_sources, overwrite = TRUE)


#
# Dataset colname
#

dataset_info <- list(dataset_colname = "Dataset",
                     valid_from_version_colname = "ValidFromVersion",
                     valid_to_version_colname = "ValidToVersion",
                     iea = "CL-PFU IEA",
                     mw = "CL-PFU MW",
                     both = "CL-PFU IEA+MW")
usethis::use_data(dataset_info, overwrite = TRUE)


#
# Additional hash group columns
#

usual_hash_group_cols <- c(dataset = dataset_info$dataset_colname,
                           table_name = hashed_table_colnames$db_table_name,
                           country = IEATools::iea_cols$country,
                           method = IEATools::iea_cols$method,
                           year = IEATools::iea_cols$year,
                           last_stage = IEATools::iea_cols$last_stage,
                           energy_type = IEATools::iea_cols$energy_type,
                           includes_neu = Recca::psut_cols$includes_neu,
                           tar_group = "tar_group")
usethis::use_data(usual_hash_group_cols, overwrite = TRUE)


#
# Aggregation file information
#

aggregation_file_tab_names <- list(region_aggregation = "region_aggregation",
                                   continent_aggregation = "continent_aggregation",
                                   world_aggregation = "world_aggregation",
                                   ef_product_aggregation = "ef_product_aggregation",
                                   eu_product_aggregation = "eu_product_aggregation",
                                   ef_sector_aggregation = "ef_sector_aggregation")
usethis::use_data(aggregation_file_tab_names, overwrite = TRUE)


aggregation_file_cols <- list(many_colname = "Many",
                              few_colname = "Few")
usethis::use_data(aggregation_file_cols, overwrite = TRUE)


#
# Aggregation data frame columns
#

aggregation_df_cols <- list(product_aggregation = "ProductAggregation",
                            industry_aggregation = "IndustryAggregation",
                            specified = "Specified",
                            despecified = "Despecified",
                            ungrouped = "Ungrouped",
                            grouped = "Grouped",
                            chopped_mat = "ChoppedMat",
                            chopped_var = "ChoppedVar",
                            product_sector = Recca::aggregate_cols$product_sector)
usethis::use_data(aggregation_df_cols, overwrite = TRUE)


#
# Metadata information for aggregation groups
#

agg_metadata <- list(total_value = "Total",
                     all_value = "All",
                     product_value = "Product",
                     sector_value = "Sector",
                     flow_value = "Flow",
                     none = "None")
usethis::use_data(agg_metadata, overwrite = TRUE)


#
# Unwrapped matrix column names.
# These were formerly i, j, and x.
#

mat_colnames <- list(i = "i",
                     row = "i",
                     j = "j",
                     col = "j",
                     x = "value",
                     value = "value")
usethis::use_data(mat_colnames, overwrite = TRUE)


#
# Give names for matrix meta information columns
#

mat_meta_cols <- list(matname = "matname",
                      matval  = "matval",
                      rowname = "rowname",
                      colname = "colname",
                      rowtype = "rowtype",
                      coltype = "coltype")
usethis::use_data(mat_meta_cols, overwrite = TRUE)
