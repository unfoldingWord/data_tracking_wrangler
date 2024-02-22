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
library(janitor, warn.conflicts = FALSE)
library(jsonlite, warn.conflicts = FALSE)
library(dotenv)

#### This chunk pulls in all of the data from the various data sources. Data cleaning and joining is done to result in a final granular datatable that can be used for analysis 
#load_dot_env(file = '.env')

print("Loading internal data tables into pipeline.")

# Loads in uW's Internal DB
connObj <- dbConnect(MySQL(),	 user=Sys.getenv("TDB_USER"),	 password=Sys.getenv("TDB_PASSWORD"),	
                     dbname=Sys.getenv("TDB_DB"), host=Sys.getenv("TDB_HOST"), default.file="/app/my.cnf", groups="security")

# Check security of connection
#rs_secure = dbGetQuery(connObj, "SHOW STATUS LIKE 'Ssl_cipher';")
#print(as.character(rs_secure[1,2]))

# Sets the characters to utf8 format so that special characters do not break the data
rs <- dbSendQuery(connObj, 'set character set "utf8"')
# Results in the following tables being loaded from the MySQL DB
ietf_lang <- DBI::dbGetQuery(connObj, "select * from ietf_languages_codes;") # A list of IETF codes that we maintain
lang_data <- DBI::dbGetQuery(connObj, "select * from Language_Data_Grid_view;") # Language engagement data that are known or planned languages our network teams connect with
uw_country <- DBI::dbGetQuery(connObj, "select * from country_data;") # A curated list of countries, their world regions, and standard country codes
networks <- DBI::dbGetQuery(connObj, "select * from network_information;") # A list of networks uW is connected with
sli <- DBI::dbGetQuery(connObj, "select * from SLI_Resource_List_v10_Grid_view;")  # Version 10 of the Strategic Language Initiative. Lists the latest Resource Levels per SL
bible_chpt <- DBI::dbGetQuery(connObj, "select * from BibleChapters;") # A curated list of Bible information, chapter #s, verse #s, and cannonical ordering
bt_data <- DBI::dbGetQuery(connObj, "select * from bt_project_data;") # The granular data of BT tracking and progress across each network and language. Most granular level of our data, which goes to unique resources and formats
pb <- DBI::dbGetQuery(connObj, "select * from progress_bible;") # Progress Bible All Access Goal List
training_data <- DBI::dbGetQuery(connObj, "select * from training_events;") %>% # Load in Rough draft training data
  dplyr::filter(`Category of Training` != "Networks PR")
slim_jp <- DBI::dbGetQuery(connObj, "select * from slim_jp;") # Loads in a skinny version of the Joshua's Project Table

DBI::dbDisconnect(connObj)

print("Preparing to create analysis tables.")

######################### Cleaning up the dates with the BT Data ##############################
# Some coerced date warnings are suppressed but it occurs only because of the logic below. The final result of the code leaves clean usable date fields under the names new_start and new_published
suppressWarnings(
  bt_data <- bt_data %>%
    mutate(correct_st_date = as_date(start_date),
           fixing_st_date = mdy(start_date),
           new_start = case_when(!is.na(fixing_st_date) ~ fixing_st_date ,
                                 !is.na(correct_st_date) ~ correct_st_date),
           correct_pb_date = as_date(published_date),
           fixing_pb_date = mdy(published_date),
           new_published = case_when(!is.na(fixing_pb_date) ~ fixing_pb_date,
                                     !is.na(correct_pb_date) ~ correct_pb_date),
    ) %>% 
    select(-c(correct_st_date,fixing_st_date,correct_pb_date,fixing_pb_date,start_date,published_date))
)
###############################################################################################


# Language Engagement is joined to the IETF Language List as well as the SLI List and Country List 
lang_ietf <- inner_join(lang_data, ietf_lang) %>%
  left_join(sli, by = c("subtag_new" = "BCP47")) %>%
  select(lang_Id, ietf_id, primary_anglicized_name, alternative_names, subtag_new, `Resource Level`, is_gl,
         country_name, u_w_network_rep, translating_organization,`ISO 639_2`) %>%
  left_join(uw_country, by = c("country_name" = "english_short_name")) %>%
  select(-alpha_3_code)



########################################################################################################################
##### Preparing and cleaning the Network's datatable so that it can be properly joined to the Language Engagement ######
########################################################################################################################
# ** This is required because the translation organization field is a JSON field contained within a R data table and does not read easily. So it needs to be parsed out and then cleaned **
clean_lang_to <- lapply(1:nrow(lang_ietf), function(x) {
  
  data.frame(lang_ietf[x,c(1:8,10:12)],"org_names" = fromJSON(as.character(lang_ietf[x,"translating_organization"])),
             row.names = NULL) 
})
# splits up the JSON column into a one to many relationship between 1 language with many translating organizations
clean_lang_to <- do.call(rbind,clean_lang_to) %>%
  select(everything(), "translating_organization_clean" = org_names) %>%
  # Joins in the network information per row to see the sensitivity per network
  left_join(networks, by = c("translating_organization_clean" = "net_Id")) %>%
  select(lang_Id,"translating_organization" = translating_organization.y, sensitivity) %>%
  # Groups on the language id to then do a logic test as a sensitivity hierarchy, if there are two networks and one is confidential and another restricted, it should default to highest sensitivity
  group_by(lang_Id) %>%
  mutate(new_sensitivity = case_when(any(sensitivity %in% "Confidential") ~ "Confidential",
                                     any(sensitivity %in% "Restricted") ~ "Restricted",
                                     any(sensitivity %in% "Unrestricted") ~ "Unrestricted"),
         concat_to = paste(translating_organization, collapse = ", ")) %>%
  select(lang_Id,"translating_organization" = concat_to,"sensitivity" = new_sensitivity) %>%
  # brings those records back to unique one to one relationships, preparing it to be joined into the language_ietf datatable
  unique()
########################################################################################################################

# Pull in Corrected Network data into the Language Engagement data to now make it a complete uW language table.
lang_ietf_network <- inner_join(lang_ietf, clean_lang_to, by = "lang_Id") %>%
  select(-translating_organization.x) %>%
  select(everything(),"translating_organization" = translating_organization.y)

# Tying in Progress Bible All Access Goals into uW's language data and creating two variables a combined naming for each language and the elligibilty for AAG
lang_ietf_pb <- lang_ietf_network %>%
  left_join(pb, by = c("ISO 639_2" = "LanguageCode")) %>%
  mutate(AAG_Elligible = case_when(AllAccessStatus %in% c("Translation in Progress","Translation Not Started","Not Shown") ~ "Yes",
                                   is.na(AllAccessStatus) ~ "Unknown" ,
                                   T ~ "No"),
         "combined_anglicized_name" = case_when(!is.na(primary_anglicized_name) & alternative_names == "" ~ primary_anglicized_name,
                                                !is.na(primary_anglicized_name) & alternative_names != "" ~ paste(primary_anglicized_name,alternative_names, sep = ", ")),
         "completed_scripture" = case_when(!is.na(CompletedScripture) & !is.na(LatestPublicationYear) ~ paste(CompletedScripture,LatestPublicationYear, sep = " - "),
                                           !is.na(CompletedScripture) & is.na(LatestPublicationYear) ~ paste(CompletedScripture," - No Date Associated"),
                                           T ~ NA_character_)) %>%
  select(lang_Id,ietf_id,primary_anglicized_name,alternative_names,subtag_new,`Resource Level`,is_gl,country_name,u_w_network_rep,
         translating_organization,`ISO 639_2`,world_region,alpha_2_code,sensitivity,
         ActiveTranslation, AllAccessStatus,AllAccessGoal,AAG_Elligible,combined_anglicized_name,completed_scripture)


# This table flips the join and looks at progress bible specific records and adds on uW engagement records to see the full list of AAG languages and where there is potential opportunity for uW to add language projects
pb_w_uw_engagement <- lang_ietf_network %>%
  right_join(pb, by = c("ISO 639_2" = "LanguageCode")) %>%
  mutate(AAG_Elligible = case_when(AllAccessStatus %in% c("Translation in Progress","Translation Not Started","Not Shown") ~ "Yes",
                                   is.na(AllAccessStatus) ~ "Unknown" ,
                                   T ~ "No"),
         has_uw_engagement = case_when(!is.na(lang_Id) ~ "Yes",
                                       T ~ "No")
  ) %>%
  select(LanguageName,`ISO 639_2`, Country, AllAccessStatus,AllAccessGoal,AAG_Elligible,has_uw_engagement)

# Two lists are created to show what is OT or NT for connections of the Bible books
ot_list <- bible_chpt %>% dplyr::filter(Bible_Section == "OT") %>% select(Scripture_Association) %>% unlist(use.names = F)
nt_list <- bible_chpt %>% dplyr::filter(Bible_Section == "NT") %>% select(Scripture_Association) %>% unlist(use.names = F)

# This is the prep data table that pulls everything together and prepares to be ready for full analysis
analysis_prep_dt <- inner_join(bt_data, lang_ietf_pb) %>%
  # Bible book reference connects which books are associated with what part of scripture. Stories, Select passages, OT, NT or the complete Bible
  mutate("bible_book_ref" = case_when(resource_package == "OBS" ~ "STORY",
                                      resource_package == "EJ" ~ "SELECT",
                                      (resource_package == "Scripture Text" | resource_package == "BP") &
                                        scriptural_association %in% ot_list ~ "OT",
                                      (resource_package == "Scripture Text" | resource_package == "BP") &
                                        scriptural_association %in% nt_list ~ "NT",
                                      (resource_package == "Scripture Text" | resource_package == "BP") &
                                        scriptural_association == "Bible" ~ "BIBLE"))
# This groups the data by specific strata in order to calculate a Status of the scripture as a whole project. This dataset will be used for the Funding Team report because it looks at the BT projects as a whole set when viewing the status of the project.
# This creates the master data table which brings all of the data together on the most granular level. The networks, language engagement, and progress bible data all gets attributed to each record that has detailed variables of progress in bible translation projects. This will be the table moving forward that is used for analysis
Bible_sets <- analysis_prep_dt %>%
  group_by(country_name,combined_anglicized_name,resource_package,resource_format,bible_book_ref) %>%
  mutate(BP_num = n(),
         # Reconfiguring the status so that it is aggregated to the Bible, OT, NT level even if it involved Book packages as parts.
         project_status = case_when(resource_package == "BP" & (bible_book_ref == "NT" | bible_book_ref == "OT")  & any(status == "In Progress") ~ "Active",
                            # If Book Packages are completed but the full set of completed BPs is not 27 or 39 then it is listed as active/in progress
                            resource_package == "BP" & bible_book_ref == "NT" & all(status == "Completed") & BP_num < 27 ~ "Active",
                            resource_package == "BP" & bible_book_ref == "OT" & all(status == "Completed") & BP_num < 39 ~ "Active",
                            resource_package == "Scripture Text" & bible_book_ref == "NT" & scriptural_association == "NT" & all(status == "Completed") ~ "Completed",
                            resource_package == "Scripture Text" & bible_book_ref == "OT" & scriptural_association == "OT" & all(status == "Completed")  ~ "Completed",
                            resource_package == "Scripture Text" & bible_book_ref == "NT" & scriptural_association %in% nt_list & all(status == "Completed") & BP_num < 27 ~ "Active",
                            resource_package == "Scripture Text" & bible_book_ref == "OT" & scriptural_association %in% ot_list & all(status == "Completed") & BP_num < 39 ~ "Active",
                            status == "Planned" | status == "Not Scheduled" ~"Future",
                            any(status == "In Progress") ~"Active",
                            status %in% c("Paused") ~"Paused",
                            status %in% c("Inactive") ~"Inactive",
                            all(status == "Completed") ~"Completed")) %>%
  ungroup()


# Parses out the unique project status for each language and condenses them into a single concatenated field of statuses per single language. This will be used to bring in engagement status into the lang_ietf_pb table. The goal is so that we would know language engagements that are currently in an active status.
concatenated_status <- Bible_sets %>% select(subtag_new,project_status) %>% unique() %>% group_by(subtag_new) %>% arrange(project_status) %>%
  pivot_wider(names_from = project_status, values_from = project_status) %>%
  mutate(logic_status = case_when(Active == "Active" ~ "Active",
                                  Future == "Future" ~ "Future",
                                  Paused == "Paused" ~ "Paused",
                                  Completed == "Completed" ~ "Completed",
                                  Inactive == "Inactive" ~ "Inactive",
                                  T ~ NA

  )) %>%
  select(subtag_new, "project_status" = logic_status) %>% unique()

lang_ietf_pb <- lang_ietf_pb %>%
  left_join(concatenated_status) %>% 
  mutate(project_status = case_when(is.na(project_status) ~ "No Status Listed",
                                    T ~ project_status))


print("Preparing to load analysis tables into the internal DB")

# Importing Analysis tables into internal uW DB
connObj <- dbConnect(MySQL(),	 user=Sys.getenv("TDB_USER"),	 password=Sys.getenv("TDB_PASSWORD"),	
                     dbname=Sys.getenv("TDB_DB"), host=Sys.getenv("TDB_HOST"), default.file="/app/my.cnf", groups="security")

rs <- dbSendQuery(connObj, 'set character set "utf8"')

# Check security of connection
#rs_secure = dbGetQuery(connObj, "SHOW STATUS LIKE 'Ssl_cipher';")
#print(as.character(rs_secure[1,2]))

RMySQL::dbWriteTable(connObj,"analysis_full_lang_progress_bible", lang_ietf_pb, overwrite=T, row.names = FALSE)
RMySQL::dbWriteTable(connObj,"analysis_granular_bt_project", Bible_sets, overwrite=T, row.names = FALSE)
RMySQL::dbWriteTable(connObj,"progress_bible_w_uw_engagement", pb_w_uw_engagement, overwrite=T, row.names = FALSE)

DBI::dbDisconnect(connObj)

print("Loading finished. Pipeline has completed")
