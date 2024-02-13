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
# Beatles tables
#

beatles_file_path <- file.path("data-raw", "BeatlesSchema.xlsx")
beatles_schema_table <- load_schema_table(schema_path = beatles_file_path,
                                          schema_sheet = "Schema")

usethis::use_data(beatles_schema_table, overwrite = TRUE)

beatles_fk_tables <- load_simple_tables(simple_tables_path = beatles_file_path,
                                        readme_sheet = "README",
                                        schema_sheet = "Schema")

usethis::use_data(beatles_fk_tables, overwrite = TRUE)
