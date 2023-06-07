### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ###
#                                                                    #
#   Match IBS mill dataset with Trase refineries                     #
#                                                                    #
#   Input:  - IBS-UML panel                                          #
#             --> IBS_UML_panel_final.dta                            #
#                                                                    #
#           - IBS mills, each of the year it has the most recent     #
#             valid desa id, with the corresponding geometries       #
#             --> IBSmills_desageom.Rdata                            #
#                                                                    #
#           - Manufacturing directories                              #
#             --> direktori_industri_merged_cleaned.xlsx             #
#                                                                    #
#           - Trase georeferenced refineries                         #
#             --> trase_refineries.xlsx                              #
#                                                                    #
#   Output: several subsets of IBS, depending on how they            #
#           matched with refineries.                                 #

### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ###

## WORKING DIRECTORY SHOULD BE CORRECT IF THIS SCRIPT IS RUN WITHIN R_project_for_individual_runs
## OR CALLED FROM LUCFP PROJECT master.do FILE.
## IN ANY CASE IT SHOULD BE (~/LUCFP/data_processing) 

# Create a directory to store outputs from this script on REFINERIES, in isolation from the outputs from the mill geolocalization work. 
dir.create("temp_data/processed_refinery_geolocalization") 

## Workstation set-up ------------------------------------------------------
library(tidyverse)
# library(plyr)
library(aws.s3)
library(sf)
library(sjmisc)
library(Hmisc)
library(readxl)
library(xlsx)
library(foreign)
library(readstata13)
# library(stringr)
aws.signature::use_credentials()
Sys.setenv("AWS_DEFAULT_REGION" = "eu-west-1")

## INPUTS ------------------------------------------------
# Trase refineries 
trase <- read_excel(file.path("input_data", "REFS_14112021.xlsx"))

# final IBS-UML panel 
ibs_uml <- read.dta13(file.path("input_data", "IBS_UML_panel_final.dta"))

# Tabularized manufacturing directories
md <- read_excel(file.path("input_data", "manufacturing_directories", "direktori_industri_merged_cleaned.xlsx"))

# IBS desa geometries 
ibs_desa <- readRDS(file.path("input_data", "IBSmills_desageom.Rdata"))

## PREPARE IBS DATA -------------------------------------

# Remove IBS manufactories that are matched with Universal Mill List (UML)
# --> KEEP only those firm_id that were NOT matched with UML, be they in the IBS_UML_panel_final (==0) or not (NA)
ibs_noUML <- dplyr::filter(ibs_uml, ibs_uml$uml_matched_sample == 0) 
# sum(ibs_noUML_cs$uml_matched_sample)  # 470
# sum(ibs_noUML_cs$is_mill) # 930 (they sourced FFB at least once or sold CPO or PKO at least once, and that they are not located in Java or in Bali)

#### Cross-sectional variables ####  
# collapse to cross section, based on some summarized values
ibs_noUML <- 
  ibs_noUML %>% 
  group_by(firm_id) %>% 
  mutate(uml_matched_sample = unique(uml_matched_sample), 
          is_mill = unique(is_mill),
          # mill_names = unique(mill_name),
          workers_nbs = list(unique(workers_total_imp3)),
          avg_in_ton_cpo_imp1 = mean(in_ton_cpo_imp1, na.rm = TRUE), 
          avg_out_ton_rpo_imp1 = mean(out_ton_rpo_imp1, na.rm = TRUE),
          avg_out_ton_rpko_imp1 = mean(out_ton_rpko_imp1, na.rm = TRUE),
          min_in_ton_cpo_imp1 = min(in_ton_cpo_imp1, na.rm = TRUE), 
          min_out_ton_rpo_imp1 = min(out_ton_rpo_imp1, na.rm = TRUE),
          min_out_ton_rpko_imp1 = min(out_ton_rpko_imp1, na.rm = TRUE),
          max_in_ton_cpo_imp1 = max(in_ton_cpo_imp1, na.rm = TRUE), 
          max_out_ton_rpo_imp1 = max(out_ton_rpo_imp1, na.rm = TRUE),
          max_out_ton_rpko_imp1 = max(out_ton_rpko_imp1, na.rm = TRUE)) %>% 
  ungroup() %>% 
# THE WARNINGS are for plants that have only NAs on some of these variables

  mutate(any_rpo = is.finite(avg_out_ton_rpo_imp1) & max_out_ton_rpo_imp1>0, 
        any_rpko = is.finite(avg_out_ton_rpko_imp1) & max_out_ton_rpko_imp1>0, 
        any_incpo = is.finite(avg_in_ton_cpo_imp1) & max_in_ton_cpo_imp1>0,
        inout_refinery = (any_rpo | any_rpko | any_incpo))

# ibs_noUML %>%
#   filter(is.finite(min_in_ton_cpo_imp1)) %>% 
#   select(firm_id, year, in_ton_cpo_imp1, min_in_ton_cpo_imp1) %>% View()

# rm(ibs_uml, ibs_noUML_cs)

#### MD variables, matched by workers ####

# This is a preliminary step to be able to resolve conflicts in spatial matches between Trase refineries and IBS 
# In the IBS-UML matching (i.e., for mills) this was done solely for those IBS mills that 
# had a desa polygon but matched with no uml mills (called "unref")

unique(md$main_product)

sjmisc::str_contains(unique(md$main_product), "olahan", ignore.case = T)
sjmisc::str_contains(unique(md$main_product), "goreng", ignore.case = T)

md[md$main_product=="MINYAK GORENG",]

names(md) <- paste0("md_", names(md))

# USE THE PANEL VERSION: 
# Match to every IBS record (i.e. firm_id-year), all the records of MD that have the same number of workers (i.e. be it the same year or not, same district or not)
#this adds a list column. There is one list element for each unref row. 
ibs_md <- dplyr::nest_join(ibs_noUML, md, by = c("workers_total_imp3" = "md_no_workers"), keep = TRUE, name = "y")
rm(ibs_noUML)

# each list element is a dataframe
class(ibs_md$y[[1]])

## Restrict to matches that are in the same district
#some cleaning of the district name variable (no need to bother about case, bc its handled in the function)
ibs_md$district_name <- str_replace(string = ibs_md$district_name, pattern = "Kab. ", replacement = "")
# not elegant but enables that empty district_names are not matched with adresses in MD
ibs_md$district_name[ibs_md$district_name == ""] <- "123456789xyz"

ibs_md$n_match <- NA
for(i in 1:nrow(ibs_md)){
  #specified as such (with switch = T), the function checks whether x is in any of the patterns (weird phrasing but that's the way to go with this function)
  kab_filter <- str_contains(x = ibs_md$district_name[i], pattern = ibs_md$y[[i]]$md_address, ignore.case = TRUE, switch = TRUE)
  # it's null if address is empty. In this case, we don't want to ibs_md
  if(length(ibs_md$y[[i]]$md_address)==0){kab_filter <- FALSE}
  # keep only the matches that are in the same district
  ibs_md$y[[i]] <- dplyr::filter(ibs_md$y[[i]], kab_filter)
  # report the number of different refineries that matched
  ibs_md$n_match[i] <- length(unique(ibs_md$y[[i]]$md_company_name)) 
}

## Make different categories of firms ("plants"), depending on how many different matches they have over their records with MD plants

# make groups of plants, based on how many matches they have repeatedly
grp_n_match <- 
  ibs_md %>% 
  group_by(firm_id) %>% 
  summarise(# those plants that never ibs_md
           no_md_match = length(n_match[n_match == 0])==length(year), 
           # one_md_match category allows for some records, but not for all, to have zero ibs_md. 
           # single matches can be different from one year to another. 
           one_md_match = length(n_match[(n_match == 1 | n_match == 0) & no_md_match == FALSE])==length(year), 
           # svl_md_match is true as soon as their is at least one year with 2 different matches. 
           svl_md_match = no_md_match == FALSE & one_md_match == FALSE) 

ibs_md <- merge(ibs_md, grp_n_match, by = "firm_id") 

# ibs_md %>% dplyr::select(firm_id, year, y, n_match, no_md_match, one_md_match, svl_md_match) %>% View()

# substract from the one_md_match category those that have different company name matches across years.
for(i in unique(ibs_md[ibs_md$one_md_match == TRUE, "firm_id"])){
  # extract the names of all the MD matches of plant i within the one_md_match category 
  names <- lapply(ibs_md[ibs_md$firm_id == i, "y"], function(i.elmt) i.elmt$md_company_name)
  names <- unlist(names)
  # for those who have matched different company names across years, switch one_md_match from TRUE to FALSE 
  new_logicals <- rep(FALSE, nrow(ibs_md[ibs_md$firm_id == i,]))
  ibs_md[ibs_md$firm_id == i, "one_md_match"][length(unique(names)) > 1] <- new_logicals
  # for those who have matched different company names across years, switch svl_md_match from FALSE to TRUE
  new_logicals <- rep(TRUE, nrow(ibs_md[ibs_md$firm_id == i,]))
  ibs_md[ibs_md$firm_id == i, "svl_md_match"][length(unique(names)) > 1] <- new_logicals
}
# checks: 
# when the company name is the same across annual matches, the one_md_match status remains unchanged
ibs_md[ibs_md$firm_id == 2028, "y"]
grp_n_match[grp_n_match$firm_id == 2028,]
ibs_md[ibs_md$firm_id == 2028, c("no_md_match","one_md_match", "svl_md_match")]

# when the company name is not the same across annual matches, the one_md_match status changes from TRUE to FALSE and the svl_md_match from FALSE to TRUE
ibs_md[ibs_md$firm_id == 3292, "y"]
grp_n_match[grp_n_match$firm_id == 3292,]
ibs_md[ibs_md$firm_id == 3292, c("no_md_match","one_md_match", "svl_md_match")]

# descriptive part
describe(ibs_md[ibs_md$no_md_match == TRUE, "firm_id"]) # 821 plants (2426 records)
describe(ibs_md[ibs_md$one_md_match == TRUE, "firm_id"]) # 118 plants (472 records)
describe(ibs_md[ibs_md$svl_md_match == TRUE, "firm_id"]) # 64 plants (356 records)

ibs_md$i_n_match <- "never matches with anything"
ibs_md$i_n_match[ibs_md$one_md_match == TRUE] <- "matches always with the same company name or with nothing"
ibs_md$i_n_match[ibs_md$svl_md_match == TRUE] <- "matches with several company names, either the same year or across years"

summarise(ibs_md, .by = c(i_n_match,inout_refinery), 
      n_mills = length(unique(firm_id)))

# la question est est-ce qu'on décide de valider systématiquement les cas où il n'y a zéro ou qu'un seul ibs_md toujours identique entre les années d'une mill ibs. 
# on pourrait dire : oui a condition qu'il y ait au moins deux occurrences de ce ibs_md. 
# ou même pas, manuellement on avait validé même quand il n'y avait qu'une obs. qui matchait.
# une partie de ces cas sont écartés ensuite pendant la phase de résolution des conflits. 

ibs_md <- ibs_md %>% 
  mutate(wtn_cfl_names = ibs_md$svl_md_match == TRUE & ibs_md$inout_refinery == TRUE)

length(unique(ibs_md$firm_id))

# Prepare the data to resolve "within" conflicts, i.e. conflicts in matched company names within each firm_id. 
# add variables on the total different matches for one ibs plant  
ibs_md$diff_names <- rep(NA, nrow(ibs_md))
ibs_md$n_diff_names <- rep(NA, nrow(ibs_md))
for(i in unique(ibs_md$firm_id)){
  names <- lapply(ibs_md[ibs_md$firm_id == i, "y"], function(i.elmt) i.elmt$md_company_name)
  ibs_md[ibs_md$firm_id == i, "diff_names"] <- paste(unique(unlist(names)), collapse = "; ")
  ibs_md[ibs_md$firm_id == i, "n_diff_names"] <- length(unique(unlist(names)))
}

ibs_md_panel <- ibs_md
# # REMOVE TEMPORAL DIMENSION - NO, because it happens that the firm_id of a plant changes through time
ibs_md <-
  ibs_md %>%
  distinct(firm_id, y, .keep_all = TRUE) %>%
  select(-year)

nrow(ibs_md)

# RESHAPE TO LONG ON MD MATCHES
# Unnest to keep info on main product, in addition to company name info 
ibs_md_long <- 
  ibs_md %>% 
  tidyr::unnest(cols = y, 
                keep_empty = TRUE)

nrow(ibs_md_long)
# rm(ibs_md, grp_n_match)


#### Desa geom #### 

# - note that this is a cross section
nrow(ibs_desa[!duplicated(ibs_desa$firm_id),]) == nrow(ibs_desa)
# and it goes only to 2010 at the latest. The desa variable was not shipped for years after 2010.
# so missing geometries are not recent firms. 
ibs_desa <- dplyr::select(ibs_desa, firm_id, geom) 
# import geo names from here, because they are the most recent valid ones. 
ibs_desa <- dplyr::filter(ibs_desa, !is.na(geom))

ibs_desa <- st_as_sf(ibs_desa, crs = 4326)

# append it to the main data 
ibs_md_long_desa <- left_join(ibs_md_long, ibs_desa, by = "firm_id")


# Keep only useful variables
ibs_md_long_desa <- 
  ibs_md_long_desa %>% 
  select(firm_id, min_year, max_year,
                out_ton_rpo_imp1, out_ton_rpko_imp1, in_ton_cpo_imp1, 
                # starts_with("any_"), 
                starts_with("avg_"), 
                # starts_with("min_"), 
                starts_with("max_"), 
                inout_refinery,
                md_main_product,
                diff_names, md_company_name, md_year, n_diff_names, 
                n_match, i_n_match, 
                wtn_cfl_names, no_md_match, one_md_match, svl_md_match,
                workers_nbs,  md_no_workers,
                district_name, kec_name, village_name, 
                geom
  )
# in_ton_ffb_imp1,	in_ton_ffb_imp2, out_ton_cpo_imp1,	out_ton_cpo_imp2,	out_ton_pko_imp1, out_ton_pko_imp2,	
# out_ton_rpo_imp1, out_ton_rpo_imp2, out_ton_rpko_imp1, out_ton_rpko_imp2,
# pct_own_cent_gov_imp,	pct_own_loc_gov_imp,	pct_own_nat_priv_imp,	pct_own_for_imp)

# label IBS variables 
names(ibs_md_long_desa)[!grepl("md_", names(ibs_md_long_desa))] <- 
  paste0("ibs_", names(ibs_md_long_desa)[!grepl("md_", names(ibs_md_long_desa))])
# # this is necessary for sf to keep recognizing the geom column
# st_geometry(ibs_md_long_desa) <- "ibs_geom"


# split the data set into those with valid desa polygon, and those without (either bc they appeared after 2010, or because they have an invalid polygon)

# these are those that cannot be matched with spatial help 
ibs_md_long_nodesa <- dplyr::filter(ibs_md_long_desa, st_is_empty(ibs_geom)) 
# remove the geom column from the other objects
ibs_md_long_nodesa <- dplyr::select(ibs_md_long_nodesa, -ibs_geom)

# make those with desa a spatial data frame 
ibs_md_long_desa <- dplyr::filter(ibs_md_long_desa, !st_is_empty(ibs_geom)) #

ibs_md_long_desa <- st_as_sf(ibs_md_long_desa)
ibs_md_long_desa



## TRASE-IBS MATCH, BY DESA ####

#### Spatial join #### 

# prepare Trase refineries dataset
trase$latitude <- as.numeric(trase$latitude)
trase$longitude <- as.numeric(trase$longitude)

trase <- st_as_sf(trase,	coords	=	c("longitude",	"latitude"), crs = 4326, remove = FALSE)

fullspj <- st_join(x = trase, y = ibs_md_long_desa, left = T) # the default is st_intersect.
# it's a left join, so it keeps all refineries from x, and only those of y that match.  
# trase_ids <- unique(trase$ref_id)
# all(trase_ids %in% unique(fullspj$ref_id))

#plot(st_geometry(ibs_noUML[ibs_noUML$firm_id == 1761,]))
#plot(st_geometry(uml[uml$mill_name == "PT. Socfin - Seumanyam",]), add = TRUE, col = "red")

# plot(st_geometry(trase))
# plot(st_geometry(ibs_md_long_desa), add = T)
fullspj <- st_drop_geometry(fullspj)


#### Spot cases where the same refinery has had different firm_id in IBS ####
pot_svl_id <- 
  fullspj %>% 
  group_by(trase_id) %>% 
  filter(min(ibs_max_year) < max(ibs_min_year)) %>% 
  ungroup() %>% 
  left_join(ibs_md_panel, by = c("ibs_firm_id" = "firm_id"), 
            multiple = "all")

pot_svl_id %>% 
  mutate(across(.cols = contains("_ton_"), .fns = round)) %>% 
  select(trase_id, comp_name, md_company_name,
         ibs_firm_id, year, 
         ibs_min_year, ibs_max_year, 
         ibs_inout_refinery, 
         in_ton_cpo_imp1, out_ton_rpo_imp1, out_ton_rpko_imp1, in_ton_ffb_imp1, out_ton_cpo_imp1) %>% 
  arrange(trase_id, ibs_firm_id, year)%>% 
  View()

# 50460 is just not a refinery apparently. Thus, no need to merge it with 54263, as it will not create a conflict. 
# And we don't want to spuriously add info on other variables (like ownership, ffb or cpo) to the final trase-IBS refinery data set.  

# In some cases, no hint that it's the same plant with different firm_id, but we still want to discard 
# some firm_id from being matched with trase refineries, since they only have one year record, 
# while another that matches has more for instance
fullspj <-
  fullspj %>% 
  mutate(adhoc_discard = case_when(
           ibs_firm_id==55623 ~ FALSE, 
           ibs_firm_id==66732 ~ FALSE, 
           TRUE ~ TRUE
         ))


#### Characterize spatial matches ####  

# NOTES! 
# 1 - The code used to characterize needs the temporal duplicates to be removed already
# 2 - This is all from the perspective of Trase refineries - not IBS. 

# There are 97 refineries
length(unique(fullspj$trase_id))
# with 73 distinct names
length(unique(fullspj$comp_name)) 

# Since we charecterize spatial matches, we look only at the subset of refineries that matched something
spj <- 
  fullspj %>% 
  filter(!is.na(ibs_firm_id))
# this is 45 Trase refineries
length(unique(spj$trase_id))

length(unique(spj$ibs_firm_id))
# They fall in the village polygon of 45 IBS plants (unique ibs_firm_id) 
# i.e. 45 IBS plants are in a village where at least one Trase refinery stands. 
## So, the maximum number of pre-2011 IBS plants we can hope to spatially match with Trase refineries is 46.
## The minimum number is 8 (unless we double-check them with workers and some don't match). 

pot_mto <- spj[!(duplicated(spj$trase_id) | 
                 duplicated(spj$trase_id, fromLast = TRUE)), ] 
length(unique(pot_mto$trase_id))
# these are the 25 potential one-to-many matches 
# (the Trase refinery is the sole one to be in the  village and 
# there may be several Trase refineries in this same village.). 

# Among them, there are:
oto <- pot_mto[!(duplicated(pot_mto$ibs_firm_id) | duplicated(pot_mto$ibs_firm_id, fromLast = TRUE)), ] 
length(unique(oto$trase_id))
# 8 Trase refineries that fall in the village of one and only one IBS plant 

mto <- pot_mto[duplicated(pot_mto$ibs_firm_id) | duplicated(pot_mto$ibs_firm_id, fromLast = TRUE), ]
length(unique(mto$trase_id))
nrow(distinct(mto, ibs_firm_id))
# 17 Trase refineries are at least 2 to fall in the village of the same IBS plant. These IBS plants are 7. 

du <- spj[(duplicated(spj$trase_id) | duplicated(spj$trase_id, fromLast = TRUE)), ]
length(unique(du$trase_id))
length(unique(du$ibs_firm_id))
# 20 trase refineries fall in a village where there are more than one IBS plant, 
# matching with 30 IBS plants.

noto <- rbind(mto, du)
length(unique(noto$trase_id))
# these are the 37 Trase refineries (17 + 20) that fall within the desa of an IBS plant and are either m:m, o:m or m:o with ibs_firm_id. 


spj_tocheck <- 
  spj %>% 
  mutate(oto = trase_id %in% oto$trase_id, 
         mto = trase_id %in% mto$trase_id, 
         du = trase_id %in% du$trase_id, 
         noto = trase_id %in% noto$trase_id, 
         
         cap_over = ibs_max_in_ton_cpo_imp1 > cap_final_mtperyr, # CAPACITY IS IN CPO APPARENTLY
         # avg_dist_to_cap = cap_final_mtperyr - (ibs_avg_out_ton_rpo_imp1 + ibs_avg_out_ton_rpko_imp1),      
         # min_dist_to_cap = cap_final_mtperyr - (ibs_max_out_ton_rpo_imp1 + ibs_max_out_ton_rpko_imp1),      
         improbable_match = cap_over | !ibs_inout_refinery, 
         
         # Round to ease manual work
         across(.cols = contains("_ton_"), .fns = round)
         ) %>% 
  
  # Resolve some conflicts programmatically: 
  filter(!(noto & improbable_match)) %>%

  select(oto, mto, du, trase_id, comp_name, ref_name, md_company_name, type, product, 
         md_main_product,
         contains("cap_"),
         ibs_firm_id, adhoc_discard,
         ibs_max_in_ton_cpo_imp1, ibs_avg_in_ton_cpo_imp1,
         ibs_max_out_ton_rpo_imp1, ibs_max_out_ton_rpko_imp1,
       
         ibs_min_year, ibs_max_year,
         everything(), 
         -aide_id, -group_name, -province, -country, -kcp, -rspo_certified, -rspo_model, 
         latitude, longitude) %>% 
  arrange(oto, mto, trase_id, ibs_firm_id)

View(spj_tocheck)

spj_tocheck <- 
  spj_tocheck %>% 
  filter(!(trase_id=="R-0003" & ibs_firm_id %in% c(43309, 54263)))%>% 
  filter(!(trase_id=="R-0003" & ibs_firm_id %in% c(43309, 54263)))

# Remove matches with IBS plants that are either producing more than known capacity, 
# or that have no cpo input or rpo/rpko output. 


# export for external inspection
write.xlsx2(spj_tocheck, file.path("temp_data", "trase_ibs_md_spatjoin.xlsx"))




## TRASE-IBS MATCH, BY COMPANY NAME ####

# need to clean md_company_name column with the same process that cleaned trase_company. 

# en attendant : 
ibs_md_long[ibs_md_long$md_company_name=="JAMPALAN BARU", "md_company_name"] <- "TANJUNG SARANA LESTARI"

# let's do an equality join
trase_ibs_by_desaname <- 
  trase_ibs_bydesa %>% 
  left_join(ibs_md_long,
            by = c("trase_company" = "md_company_name"), 
            multiple = "all")

# THIS IS ONLY THOSE WHO MATCH (0 currently)
# https://stackoverflow.com/questions/37289405/dplyr-left-join-by-less-than-greater-than-condition
trase_ibs_by_desaname <- 
  trase_ibs_bydesa %>% 
  mutate(dummy = TRUE) %>% 
  left_join(ibs_md_long %>% mutate(dummy = TRUE), 
            by = "dummy") %>% 
  filter(trase_company %in% diff_names) %>% 
  dplyr::select(-dummy)

## VARIABLES TO RESOLVE CONFLICTS ####

# We identify differently matched subsets 


## Those that have a desa polygon but match with no Trase refinery (414) ####
# It is useless to try to find them manually with the directory number of workers and uml's list. 
# we can still find their names with directories and google-search them, and/or directly spot them manually within their villages. 
ibs_unref <- left_join(x = ibs_md_long_desa, y = st_set_geometry(oto[,c("firm_id","lat")], NULL), by = "firm_id")
ibs_unref <- filter(ibs_unref, is.na(ibs_unref$lat))

ibs_unref <- left_join(x = ibs_unref, y = st_set_geometry(noto[,c("firm_id","lon")], NULL), by = "firm_id")
ibs_unref <- filter(ibs_unref, is.na(ibs_unref$lon))

ibs_unref <-dplyr::select(ibs_unref, -lon, -lat)
# (the use of "lat" and "lon" was just an arbitrary choice of non-empty variables to flag oto and noto resp.)

# filter to those likely to be refineries
head(ibs_unref)
summary(ibs_unref$any_rpo)
summary(ibs_unref$any_rpko)
summary(ibs_unref$any_incpo)
nrow(dplyr::filter(ibs_unref, any_rpo | any_rpko)) # "or rpko" adds 8 plants in addition to the 70 that sell rpo. 
nrow(dplyr::filter(ibs_unref, any_rpo & any_incpo))
nrow(dplyr::filter(ibs_unref, any_rpo | any_incpo))

# here the criterion for being a refinery is to output at least some rpo OR some rpko. 




summary(trase_ibs_by_desaname$trase_cap_mt) # this is expressed in metric tons, not in million tons, most likely 
trase_ibs_by_desaname <- dplyr::mutate(trase_ibs_by_desaname, excess_cap = max_out_ton_rpo_imp1 > trase_cap_mt)
summary(trase_ibs_by_desaname$excess_cap)





#### BETWEEN FIRM ID CONFLICTS in MD NAMES 

### now we want to resolve "between" conflicts, i.e. conflicts in matched company names between IBS firm_ids. 
# For this purpose, it is necessary to have all years for each ibs firm_id in the two categories 
# (resolved conflict cases and one_md_match cases (only those who produced CPO at least once))

## Within resolved conflicts
# import mannually done work 
wtn_cfl_resolved <- read_excel(file.path("input_data/manually_matched_ibs_uml/matching_unref/wtn_cfl_done.xlsx"))

# for each firm_id, keep only the row with the mannually deemed correct company name. 
# (because only one row per firm_id was given the resolved company name in the manual work in Excel)
wtn_cfl_resolved <- wtn_cfl_resolved[is.na(wtn_cfl_resolved$company_name)== FALSE,] 

length(unique(wtn_cfl_resolved$company_name)) 
# So there was 104 different firm_id that had within conflicting md company names. 
# For 102 of them, we could resolve the within conflict. 
# Among them, there are only 100 unique company names, meaning that there are some between conflicts. 

# rename the company name variable 
names(wtn_cfl_resolved)[names(wtn_cfl_resolved) == "company_name"] <- "within_resolved_c_name"

# merge it with the wtn_cfl data frame, 
wtn_cfl_resolved <- merge(wtn_cfl, wtn_cfl_resolved[, c("firm_id", "within_resolved_c_name")], by = c("firm_id"), all = TRUE)

# we don't keep only the records where the company_name is the one that was chosen manually, because in some cases we might  
# need records for the same mill but with a differently spelled name. 


## one_md_match cases
# one should just reproduce the procedure applied to prepare data for resolution of within conflicts. 

#keep only the one_md_match cases that are potential mills (they produced CPO at least once)
no_cfl <- match[match$one_md_match == TRUE & match$any_cpo_output == TRUE,]

# add variables on the total different matches for one ibs plant  
no_cfl$diff_names <- rep(NA, nrow(no_cfl))
no_cfl$n_diff_names <- rep(NA, nrow(no_cfl))
for(i in unique(no_cfl$firm_id)){
  names <- lapply(no_cfl[no_cfl$firm_id == i, "y"], function(i.elmt) i.elmt$company_name)
  no_cfl[no_cfl$firm_id == i, "diff_names"] <- paste(unique(unlist(names)), collapse = "; ")
  no_cfl[no_cfl$firm_id == i, "n_diff_names"] <- length(unique(unlist(names)))
}

#no_cfl[no_cfl$firm_id == 1763, "y"]

# extract all the matches from the list column 
l <- list()
for(i in 1:nrow(no_cfl)){
  s <- no_cfl[i, "y"][[1]] # there is always only one element anyways
  s$matched_firm_id <- rep(no_cfl[i, "firm_id"], nrow(no_cfl[i, "y"][[1]]))
  s$matched_year <- rep(no_cfl[i, "year"], nrow(no_cfl[i, "y"][[1]]))
  l[[i]]<- s
}
md_matches <- bind_rows(l)
rm(l)


# and merge them with the panel no_cfl
# rename first the year variable in md_matches
names(md_matches)[names(md_matches) == "year"] <- "md_year"
no_cfl <- merge(no_cfl, md_matches, by.x = c("firm_id","year"), by.y = c("matched_firm_id", "matched_year"), all = TRUE)

# export relevant variables to manual work
no_cfl <- dplyr::select(no_cfl,	firm_id, min_year, year,	workers_total_imp3,	n_diff_names, diff_names,  
                        md_year, company_name, no_workers, 
                        district_name, kec_name,	village_name, address,
                        main_product, 
                        in_ton_ffb_imp1,	in_ton_ffb_imp2, out_ton_cpo_imp1,	out_ton_cpo_imp2,	out_ton_pko_imp1, out_ton_pko_imp2,	
                        out_ton_rpo_imp1, out_ton_rpo_imp2, out_ton_rpko_imp1, out_ton_rpko_imp2,
                        pct_own_cent_gov_imp,	pct_own_loc_gov_imp,	pct_own_nat_priv_imp,	pct_own_for_imp)


## Spot the between conflicts
# have the within_resolved_c_name column in no_cfl data frame too.
no_cfl$within_resolved_c_name <- no_cfl$company_name

# merge them 
unique_match <- merge(wtn_cfl_resolved, no_cfl, all = TRUE)

# now identify duplicates across firm_id
btw_duplicates <- summarise(unique_match, .by = c("within_resolved_c_name"), 
                        btw_duplicates = length(unique(firm_id)))

unique_match <- merge(unique_match, btw_duplicates, by = "within_resolved_c_name", all = TRUE)

describe(unique_match$btw_duplicates)
# 79 firm_id have no within resolved company name. 
all_na(unique_match[unique_match$btw_duplicates == 79,"within_resolved_c_name"])

# keep only between conflicts
btw_cfl <- unique_match[unique_match$btw_duplicates > 1 & is.na(unique_match$within_resolved_c_name) == FALSE,] 

setorder(btw_cfl, within_resolved_c_name, firm_id, year)




#### Automatic conflict resolution #### 
summary(trase$cap_mt) # this is expressed in metric tons, not in million tons, most likely 
noto <- dplyr::mutate(noto, excess_cap = max_out_ton_rpo_imp1 > cap_mt)
summary(noto$excess_cap)

# They are in 455 different polygons, of which 359 do not intersect with another one
# (intersections but not equal likely when there is a split and a mill is associated with the polygon of the village before, and one or more 
#other mills are associated with the villages after the split.)
#ibs_unref$grp = sapply(st_equals(ibs_unref), max)
#ibs_unref <- arrange(ibs_unref, grp, firm_id)
#nrow(ibs_unref[unique(ibs_unref$grp),])

#unique_unref <-ibs_unref[unique(ibs_unref$grp),]

#ibs_unref$grp_int = sapply(st_intersects(ibs_unref), max)
#nrow(ibs_unref[unique(ibs_unref$grp_int),])

#plot(st_geometry(ibs_unref[ibs_unref$grp == 1 | ibs_unref$grp == 355,]), add = F)
#plot(st_geometry(ibs_unref[ibs_unref$grp == 355,]), add = F)

# also, this is 473 different year_desa_id, meaning that there are cases where two mills have equal polygons but no equal year_desa_id: these are the 
# cases of two mills being associated to the same polygon but with desa_ids from different years. 
#ibs_unref$year_desa_id <- paste(ibs_unref$year, ibs_unref$desa_id, sep = "")
#nrow(ibs_unref[unique(ibs_unref$year_desa_id),])

### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### 


#### EXPORT THEM ACCORDINGLY ####

### Those that have no desa polygon though they had an un-flagged desa_id (46)
# they return to stata to get merged with the panel and geolocalized manually. 
ibs_na <-dplyr::select(ibs_na, firm_id)
write.dta(ibs_na, "temp_data/processed_refinery_geolocalization/pre2011_bad_desa_id.dta")


### Those that have a desa polygon but match with no Trase refinery (414)
ibs_unref$desa_id <- as.character(ibs_unref$desa_id)
ibs_unref <- st_transform(ibs_unref, crs = 4326)

## For R 
saveRDS(ibs_unref, file = "temp_data/processed_refinery_geolocalization/ibs_unref.Rdata") 

## For GEE
ibs_unref <-dplyr::select(ibs_unref, firm_id, year, min_year, max_year, industry_code, out_ton_cpo_imp2,
                          workers_total_imp3, avg_in_tot_ton_cpo_imp2, avg_out_ton_cpo_imp2, avg_cpo_price_imp2, 
                          out_ton_rpo_imp1, out_ton_rpo_imp2, out_ton_rpko_imp1, out_ton_rpko_imp2,
                          desa_id, geom)

st_write(ibs_unref, "temp_data/processed_refinery_geolocalization/ibs_unref", driver = "ESRI Shapefile", delete_dsn = TRUE)
#st_write(ibs_unref,"unreferenced_mill_desa.gpkg", driver = "GPKG", delete_dsn = TRUE)

## For Stata 
ibs_unref2 <-dplyr::select(ibs_unref, firm_id)
write.dta(st_set_geometry(ibs_unref2, NULL), "temp_data/processed_refinery_geolocalization/ibs_unref.dta")


#### Those who matched (oto & noto)
## oto
oto <- st_set_geometry(oto, NULL)
oto <-dplyr::select(oto, firm_id, company, company_id, group, ref_id, ref_name, cap_mt, lat, lon)
write.dta(oto, "temp_data/processed_refinery_geolocalization/oto.dta")

## noto 
# sort by desa geometry to ease manual matching
noto$grp = sapply(st_intersects(noto), max)
noto <- arrange(noto, grp, firm_id)
noto <- st_set_geometry(noto, NULL)
noto <-dplyr::select(noto, firm_id, company, company_id, group, ref_id, ref_name, cap_mt, lat, lon, grp)
write.dta(noto, "temp_data/processed_refinery_geolocalization/noto.dta")

## Trase refineries (for GEE)
st_crs(trase) <- 4326
trase <- st_transform(trase, crs = 4326)
trase <-dplyr::select(trase, -lat, -lon)
st_write(trase, "temp_data/processed_refinery_geolocalization/trase_refineries", driver = "ESRI Shapefile", delete_dsn = TRUE)



# ***** Note ********
# Lines below are not a pb anymore, because it's ibs_noUML_cs on the left of the left join now (so keeping only final firm_id anyway)
# among the 17 firm_id that are manually changed in merging_geolocalization_works.do, 
# 13 have a desa geom and thus, are present in IBSmills_desageom.Rdata BUT NOT in IBS_UML_panel_final.dta  
# but we have identified them as being UML mills already (so we remove them here)
# nrow(ibs_noUML[is.na(ibs_noUML$uml_matched_sample),]) 
# ******   *******

unique(ibs_noUML_cs$mill_name)
# mill_name is actually useless (it has been wiped out when a firm_id was not UML)
# ibs_noUML <- dplyr::select(ibs_noUML, -mill_name)

