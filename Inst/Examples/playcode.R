library(tidyverse)
library(odbc)
library(DBI)
library(dbplyr)
library(RMySQL)
library(fmcsaAPI)

#FMCSA_CENSUS1 <- read_csv("~/GitHub/fmcsa_scraping/data/FMCSA_CENSUS1_2017Sep.zip", progress = F)
WEBKey <- 'cac7117a9b5e2b1bf64fb87da3493c69d627de1e'

con <- DBI::dbConnect(RMySQL::MySQL(),
                      host = "localhost",
                      dbname="trucking",
                      user = "root",
                      password = "gyjltzw3813"
)

US_DOT <-  "1000125"

base <- get_carrier_info(US_DOT, item = 'base', WEBKey = WEBKey) %>% tidy()
basics <- get_carrier_info(US_DOT, item = 'basics', WEBKey = WEBKey) %>% tidy()
cargo <- get_carrier_info(US_DOT, item = 'cargo-carried', WEBKey = WEBKey) %>% tidy()
ops_class <- get_carrier_info(US_DOT, item = 'operation-classification', WEBKey = WEBKey) %>% tidy()
authority <- get_carrier_info(US_DOT, item = 'authority', WEBKey = WEBKey) %>% tidy()

RMySQL::dbListTables(con)


RMySQL::dbDisconnect(con)
