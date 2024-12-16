#!/usr/bin/env Rscript


#################### 
#This script is specifically to pull in the various data resources that are available to unfoldingWord as of 1/30/2024 and create an ETL pipeline that will output data into our internal MySQL DB. This script will be ran on a scheduler daily and will populate tables that require specific business logic to be used in order to craft the data into analyzable tables.
####################

# Load the required packages
library(tidyverse)
library(lubridate)
library(tidyr)
library(DBI)
library(RMySQL)
library(janitor)
library(jsonlite)
library(dotenv)

#### This chunk pulls in all of the data from the various data sources. Data cleaning and joining is done to result in a final granular datatable that can be used for analysis 
load_dot_env(file = '.env')

print("Loading internal data tables into pipeline.")

# Loads in uW's Internal DB
connObj <- dbConnect(MySQL(),	 user=Sys.getenv("USER"),	 password=Sys.getenv("PASSWORD"),	
                     dbname=Sys.getenv("DBNAME1"), host=Sys.getenv("HOST"))

# Sets the characters to utf8 format so that special characters do not break the data
rs <- dbSendQuery(connObj, 'set character set "utf8"')

# Results in the following tables being loaded from the MySQL DB
bible_info <- DBI::dbGetQuery(connObj, "select * from bible_info;") # A curated list of Bible information, chapter #s, verse #s, and cannonical ordering
countries <- DBI::dbGetQuery(connObj, "select * from countries;") # A curated list of countries, their world regions, and standard country codes
ietf_languages_codes <- DBI::dbGetQuery(connObj, "select * from ietf_languages_codes;") # A list of IETF codes that we maintain
joshua_project_data <- DBI::dbGetQuery(connObj, "select * from joshua_project_data;") # Loads in a skinny version of the Joshua's Project Table
language_engagements <- DBI::dbGetQuery(connObj, "select * from language_engagements;") # Language engagement data that are known or planned languages our network teams connect with
language_engagements_organizations <- DBI::dbGetQuery(connObj, "select * from language_engagements_organizations;") # join table between supporting organizations and a language engagement
organizations <- DBI::dbGetQuery(connObj, "select * from organizations;") # A list of networks uW is connected with
pb_language_data <- DBI::dbGetQuery(connObj, "select * from pb_language_data;") # Progress Bible All Access Goal List
sli_language_data <- DBI::dbGetQuery(connObj, "select * from sli_language_data;")  # Version 10 of the Strategic Language Initiative. Lists the latest Resource Levels per SL
training_events <- DBI::dbGetQuery(connObj, "select * from training_events;")  # Load in Rough draft training data
uw_reps <- DBI::dbGetQuery(connObj, "select * from uw_reps;")  # A table representing all unfoldingWord network reps
uw_translation_products <- DBI::dbGetQuery(connObj, "select * from uw_translation_products;") # The granular data of BT tracking and progress across each network and language. Most granular level of our data, which goes to unique resources and formats

DBI::dbDisconnect(connObj)

print("Preparing to create analysis tables.")

######################### Cleaning up the dates with the BT Data ##############################
# Some coerced date warnings are suppressed but it occurs only because of the logic below. The final result of the code leaves clean usable date fields under the names new_start and new_published
# suppressWarnings(
#   bt_data <- bt_data %>%
#     mutate(correct_st_date = as_date(start_date),
#            fixing_st_date = mdy(start_date),
#            new_start = case_when(!is.na(fixing_st_date) ~ fixing_st_date ,
#                                  !is.na(correct_st_date) ~ correct_st_date),
#            correct_pb_date = as_date(published_date),
#            fixing_pb_date = mdy(published_date),
#            new_published = case_when(!is.na(fixing_pb_date) ~ fixing_pb_date,
#                                      !is.na(correct_pb_date) ~ correct_pb_date),
#     ) %>% 
#     select(-c(correct_st_date,fixing_st_date,correct_pb_date,fixing_pb_date,start_date,published_date))
# )
###############################################################################################


# Language Engagement is joined to the IETF Language List as well as the SLI List and Country List 
language_engage_to_ietf <- inner_join(language_engagements, ietf_languages_codes) %>%
  left_join(sli_language_data, by = c("subtag_new" = "bcp47")) %>%
  select(language_engagement_id, ietf_id, primary_anglicized_name, alternative_names, subtag_new, resource_level,
         country_id, uw_rep_id, translation_organization_id,iso_639_2) %>%
  left_join(countries, by = c('country_id' = 'alpha_3_code'))



########################################################################################################################
##### Preparing and cleaning the Network's datatable so that it can be properly joined to the Language Engagement ######
########################################################################################################################
# # ** This is required because the translation organization field is a JSON field contained within a R data table and does not read easily. So it needs to be parsed out and then cleaned **
# clean_lang_to <- lapply(1:nrow(lang_ietf), function(x) {
#   
#   data.frame(lang_ietf[x,c(1:8,10:12)],"org_names" = fromJSON(as.character(lang_ietf[x,"translating_organization"])),
#              row.names = NULL) 
# })
# # splits up the JSON column into a one to many relationship between 1 language with many translating organizations
# clean_lang_to <- do.call(rbind,clean_lang_to) %>%
#   select(everything(), "translating_organization_clean" = org_names) %>%
#   # Joins in the network information per row to see the sensitivity per network
#   left_join(networks, by = c("translating_organization_clean" = "net_Id")) %>%
#   select(lang_Id,"translating_organization" = translating_organization.y, sensitivity) %>%
#   # Groups on the language id to then do a logic test as a sensitivity hierarchy, if there are two networks and one is confidential and another restricted, it should default to highest sensitivity
#   group_by(lang_Id) %>%
#   mutate(new_sensitivity = case_when(any(sensitivity %in% "Confidential") ~ "Confidential",
#                                      any(sensitivity %in% "Restricted") ~ "Restricted",
#                                      any(sensitivity %in% "Unrestricted") ~ "Unrestricted"),
#          concat_to = paste(translating_organization, collapse = ", ")) %>%
#   select(lang_Id,"translating_organization" = concat_to,"sensitivity" = new_sensitivity) %>%
#   # brings those records back to unique one to one relationships, preparing it to be joined into the language_ietf datatable
#   unique()
########################################################################################################################

# Pull in Corrected Network data into the Language Engagement data to now make it a complete uW language table.
lang_engage_ietf_to_network <- inner_join(language_engage_to_ietf, organizations, by = c("translation_organization_id" = "org_id")) %>%
  inner_join(uw_reps, by = "uw_rep_id") %>%
  select(-c(first_name,last_name))


####################################################################################################################################################################
################# This code is extraneous and is no longer needed now that Joshua Project and Progress Bible are contained within the uW Database ##################
####################################################################################################################################################################
# #Load Progress Bible
# # Pull in through Python the API Call
# reticulate::py_run_file("pb_api.py")
# 
# # Parse out the JSON and save it as an R Dataframe
# pb_dataset <- parse_json(json = py$contents,simplifyVector = T) %>%
#   as.data.frame()
# 
# # Reset column names to be the appropriate style
# colnames(pb_dataset) <- colnames(pb_dataset) %>%
#   str_remove_all("resource.")
# 
# pb = pb_dataset
# # Pull in through Python the API Call, which results in the json being converted to a pandas df and then pulled into R
# reticulate::py_run_file("jp_api.py")
# # Pull in the pandas dataframe from python
# jp_data <- tibble(py$df)
# # Cross ref is a Joshua's Project spreadsheet that has the cross reference country codes from JP's system to ISO codes and other's codes. They have their own unique names and country codes. I tie that in with our uW country codes to make it all the same.
# cross_ref <- read_xlsx("jp_cross_ref_cntry_codes.xlsx") %>%
#   inner_join(uw_country, by = c("ISO2" = "alpha_2_code"))
# 
# jp_data <- left_join(jp_data, cross_ref, by = "ROG3")  
# # Simplified JP Data set by dropping a lot of extraneous variables
# slim_jp <- jp_data %>% select(PeopNameInCountry,english_short_name,ISO2 , LeastReached,PrimaryLanguageName,ROL3,Population)
####################################################################################################################################################################


# Tying in Progress Bible All Access Goals into uW's language data and creating two variables a combined naming for each language and the elligibilty for AAG
language_engage_ietf_orgs_reps_to_pb <- lang_engage_ietf_to_network %>%
  left_join(pb_language_data, by = c("iso_639_2" = "languagecode")) %>%
  mutate(aag_elligible = case_when(allaccessstatus %in% c("Translation in Progress","Translation Not Started","Not Shown") ~ "Yes",
                                   is.na(allaccessstatus) ~ "Unknown" ,
                                   T ~ "No"),
         "combined_anglicized_name" = case_when(!is.na(primary_anglicized_name) & alternative_names == "" ~ primary_anglicized_name,
                                                !is.na(primary_anglicized_name) & alternative_names != "" ~ paste(primary_anglicized_name,alternative_names, sep = ", ")),
         "completed_scripture" = case_when(!is.na(completedscripture) & !is.na(latestpublicationyear) ~ paste(completedscripture,latestpublicationyear, sep = " - "),
                                           !is.na(completedscripture) & is.na(latestpublicationyear) ~ paste(completedscripture," - No Date Associated"),
                                           T ~ NA_character_)) %>%
  select(language_engagement_id,ietf_id,primary_anglicized_name,alternative_names,subtag_new,resource_level,english_short_name,country_id,abbreviation,uw_rep_id,
         name,translation_organization_id,iso_639_2,world_region,alpha_2_code,sensitivity,
         activetranslation, allaccessstatus,allaccessgoal,aag_elligible,combined_anglicized_name,completed_scripture)


# Two lists are created to show what is OT or NT for connections of the Bible books
ot_list <- bible_info %>% filter(bible_section == "OT") %>% select(scriptural_association) %>% unlist(use.names = F)
nt_list <- bible_info %>% filter(bible_section == "NT") %>% select(scriptural_association) %>% unlist(use.names = F)

# This is the prep data table that pulls everything together and prepares to be ready for full analysis
analysis_prep_dt <- inner_join(uw_translation_products, language_engage_ietf_orgs_reps_to_pb) %>%
  # Bible book reference connects which books are associated with what part of scripture. Stories, Select passages, OT, NT or the complete Bible
  mutate("bible_book_ref" = case_when(resource_package == "OBS" ~ "STORY",
                                      resource_package == "EJ" ~ "SELECT",
                                      (resource_package == "Scripture Text" | resource_package == "BP") &
                                        scriptural_association %in% ot_list ~ "OT",
                                      (resource_package == "Scripture Text" | resource_package == "BP") &
                                        scriptural_association %in% nt_list ~ "NT",
                                      (resource_package == "Scripture Text" | resource_package == "BP") &
                                        scriptural_association == "Bible" ~ "BIBLE")
         ,
         scripture_text_name = case_when(scripture_text_name == "" ~ NA_character_,
                                         T ~ scripture_text_name))

# This groups the data by specific strata in order to calculate a Status of the scripture as a whole project. This dataset will be used for the Funding Team report because it looks at the BT projects as a whole set when viewing the status of the project.
# This creates the master data table which brings all of the data together on the most granular level. The networks, language engagement, and progress bible data all gets attributed to each record that has detailed variables of progress in bible translation projects. This will be the table moving forward that is used for analysis
master_uw_translation_projects <- analysis_prep_dt %>%
  group_by(english_short_name,combined_anglicized_name,resource_package,resource_format,bible_book_ref,scripture_text_name) %>%
  # group_by(english_short_name,combined_anglicized_name,resource_format,scripture_text_name) %>%
  mutate(BP_num = n(),
         project_status = case_when(
           any(product_status == "Inactive") ~"Inactive",
#           all(product_status == "Not Scheduled") ~ "Intent",
           scriptural_association %in% c("Bible","NT","OT") & product_status == "Completed" ~ "Completed",
           all(product_status == "Planned") ~ "Future",
           any(product_status == "In Progress") ~ "Active",
           resource_package == "OBS" & product_status == "Completed" ~ "Completed",
           all(product_status == "Paused") | (any(product_status == "Paused") & any(product_status == "Completed")) ~ "Paused",
           resource_package %in% c("BP","Scripture Text") & all(product_status == "Completed") & bible_book_ref == "NT" & BP_num < 27 ~ "Active",
           resource_package %in% c("BP","Scripture Text") & all(product_status == "Completed") & bible_book_ref == "OT" & BP_num < 39 ~ "Active",
           any(product_status == "Completed") & any(product_status == "Planned") ~ "Active",
           all(product_status == "Completed") ~ "Completed"
         )
  ) %>% 
  ungroup()


# Parses out the unique project status for each language and condenses them into a single concatenated field of statuses per single language. This will be used to bring in engagement status into the lang_ietf_pb table. The goal is so that we would know language engagements that are currently in an active status.
concatenated_status <- master_uw_translation_projects %>% select(subtag_new,project_status) %>% unique() %>% group_by(subtag_new) %>% arrange(project_status) %>%
  pivot_wider(names_from = project_status, values_from = project_status) %>%
  mutate(logic_status = case_when(Active == "Active" ~ "Active",
                                  Future == "Future" ~ "Future",
                                  Paused == "Paused" ~ "Paused",
                                  Inactive == "Inactive" ~ "Inactive",
#                                  Intent == "Intent" ~ "Intent",
                                  Completed == "Completed" ~ "Completed",
                                  T ~ NA
  )) %>% 
  select(subtag_new, "project_status" = logic_status) %>% unique()

master_uw_language_engagements <- language_engage_ietf_orgs_reps_to_pb %>%
  left_join(concatenated_status) %>% 
  mutate(project_status = case_when(is.na(project_status) ~ "No Status Listed",
                                    T ~ project_status))

# This table flips the join and looks at progress bible specific records and adds on uW engagement records to see the full list of AAG languages and where there is potential opportunity for uW to add language projects as well as the uW project status
pb_language_engagements <- master_uw_language_engagements %>%
  select(iso_639_2, project_status, language_engagement_id) %>%
  right_join(pb_language_data, by = c("iso_639_2" = "languagecode")) %>%
  mutate(aag_elligible = case_when(allaccessstatus %in% c("Translation in Progress","Translation Not Started","Not Shown") ~ "Yes",
                                   is.na(allaccessstatus) ~ "Unknown" ,
                                   T ~ "No"),
         has_uw_engagement = case_when(!is.na(language_engagement_id) ~ "Yes",
                                       T ~ "No")
  ) %>%
  select(languagename,iso_639_2, country, allaccessstatus,allaccessgoal,aag_elligible,has_uw_engagement,"uw_project_status" = project_status)


print("Preparing to load analysis tables into the internal DB")

# Importing Analysis tables into internal uW DB
connObj <- dbConnect(MySQL(),	 user=Sys.getenv("USER"),	 password=Sys.getenv("PASSWORD"),	
                     dbname=Sys.getenv("DBNAME1"),
                     host=Sys.getenv("HOST"))

rs <- dbSendQuery(connObj, 'set character set "utf8"')

RMySQL::dbWriteTable(connObj,"master_uw_language_engagements", master_uw_language_engagements, overwrite=T, row.names = FALSE)
RMySQL::dbWriteTable(connObj,"master_uw_translation_projects", master_uw_translation_projects, overwrite=T, row.names = FALSE)
RMySQL::dbWriteTable(connObj,"pb_language_engagements", pb_language_engagements, overwrite=T, row.names = FALSE)

DBI::dbDisconnect(connObj)

print("Loading finished. Pipeline has completed")
