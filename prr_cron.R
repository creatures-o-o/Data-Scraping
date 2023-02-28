#################
### Libraries ###
#################

# Libraries to deal with the data
library(tidyverse) # manipulating data
library(rio) # Making file downloads easy
library(rvest) # For webscraping.
library(lubridate)
library(dbplyr)
library(janitor)


##########################################################
### DHHS Rental Report by custom postcode aggregations ###
##########################################################

### About the data ###
# The source is https://www.dhhs.vic.gov.au/publications/rental-report
# Contains quarterly median rent data for every quarter from March 2000 to September 2021. Geographical area is by suburb. 
# The clean data contains the following columns:
# suburb: The suburb, which seems to be an amalgamation of postcodes in some instances. For example, North Melbourne - West Melbourne is one area.
# value: The value associated with the observation. 
# quarter: The quarter, which runs from March 2000 to September 2020. Stored as a string.
# series: Either 'No. New Lettings' or 'Moving Annual Median'. The former is the number of new lettings in the quarter. The latter represents the moving annual median (in $), not the quarterly median, so I specified this to ensure the message comes across.
# Dwelling_Type: A character string to denote the type of dwelling, ranging from 1 bedroom flat to 4 bedroom house, and all properties.

# Global variables
LGA_WRITE_DATA_NAME <- "lga_rental_reports"
LOCAL_WRITE_DATA_NAME <- "suburb_rental_reports"


######################################
### Get the latest data spreasheet ###
######################################
# Get the URL
url <- "https://www.dffh.vic.gov.au/publications/rental-report"
file_prefix <- "https://www.dffh.vic.gov.au"

# Create the website object
site <- read_html(url)

# Get the list of files (saves it as one character)
suburb_fileList <- site %>% 
  html_nodes("#page-content a") %>% # find all links on the page
  html_attr("href") %>% # get the urls of those links
  str_subset("suburb") %>%
  paste0(file_prefix,.)

lga_fileList <- site %>% 
  html_nodes("#page-content a") %>% # find all links on the page
  html_attr("href") %>% # get the urls of those links
  str_subset("quarterly") %>%
  paste0(file_prefix,.)

#############################################
### Get the latest DATE from the internet ###
#############################################

pattern <- "((March|June|September|December|march|june|september|december)-quarter-\\d\\d\\d\\d)"

filedate <- str_extract(suburb_fileList, pattern) %>%
  str_replace("quarter-", "") %>%
  tolower()


#############################################
### Get the latest data from the database ###
#############################################
# CON <- dbCon() # connect from ./dhhs_all.R

# Find return the latest data
lga_current_data <- tbl(con, in_schema("dhhs", LGA_WRITE_DATA_NAME)) %>% 
  collect()

# Find latest date in the database
local_current_data <- tbl(con, in_schema("dhhs", LOCAL_WRITE_DATA_NAME)) %>% 
  collect() 



#############################################
### Get the latest DATE from the database ###
#############################################

# Find latest date in the database
lga_latest_date <- lga_current_data %>%
  mutate(quarter = mdy(quarter))

lga_max_date <- max(lga_latest_date$quarter) %>%
  format("%B-%Y") %>%
  tolower()

if (grepl(lga_max_date, filedate, fixed = TRUE)) {
  
  print("Data already in the database")
  
  
} else {
  ####################################
  ### Function to clean the sheets ###
  ####################################
  # Create a function that cleans all the sheets
  rr_cleaner <- function(rr_file, bedrooms_text){
    
    rr_file_saved <- rr_file %>%
      dplyr::select(-x1) %>% 
      dplyr::filter(x2 != "Group Total", 
                    !is.na(x2)) %>% 
      dplyr::rename(Area = x2) %>% 
      pivot_longer(-Area, 
                   names_to = "cat1", 
                   values_to = "Value") %>% 
      mutate(cat2 = str_detect(cat1, 
                               "x")) %>% 
      mutate(Quarter = if_else(cat2 == TRUE, 
                               lag(cat1), 
                               cat1)) %>% 
      mutate(Series = if_else(cat2 == TRUE, 
                              "Moving Annual Median", 
                              "No. New Lettings")) %>% 
      dplyr::select(-cat1, 
                    -cat2) %>%
      mutate(Quarter = str_replace(Quarter, 
                                   "mar_", 
                                   "March ")) %>%
      mutate(Quarter = str_replace(Quarter, 
                                   "jun_", 
                                   "June ")) %>%
      mutate(Quarter = str_replace(Quarter, 
                                   "sep_", 
                                   "September ")) %>%
      mutate(Quarter = str_replace(Quarter, 
                                   "dec_", 
                                   "December ")) %>% 
      mutate(Value = as.numeric(Value)) %>%
      mutate(Dwelling_Type = bedrooms_text) %>%
      dplyr::rename_all(tolower)
    
  }
  
  ##############################
  ### Import the data sheets ###
  ##############################
  
  # Download and read in data
  lga_prr_1bdrf <- rio::import(lga_fileList, which = 1, skip = 1) %>% clean_names()
  lga_prr_2bdrf <- rio::import(lga_fileList, which = 2, skip = 1) %>% clean_names()
  lga_prr_3bdrf <- rio::import(lga_fileList, which = 3, skip = 1) %>% clean_names()
  lga_prr_2bdrh <- rio::import(lga_fileList, which = 4, skip = 1) %>% clean_names()
  lga_prr_3bdrh <- rio::import(lga_fileList, which = 5, skip = 1) %>% clean_names()
  lga_prr_4bdrh <- rio::import(lga_fileList, which = 6, skip = 1) %>% clean_names()
  lga_prr_allbdr <- rio::import(lga_fileList, which = 7, skip = 1) %>% clean_names()
  
  suburb_prr_1bdrf <- rio::import(suburb_fileList, which = 1, skip = 1) %>% clean_names()
  suburb_prr_2bdrf <- rio::import(suburb_fileList, which = 2, skip = 1) %>% clean_names()
  suburb_prr_3bdrf <- rio::import(suburb_fileList, which = 3, skip = 1) %>% clean_names()
  suburb_prr_2bdrh <- rio::import(suburb_fileList, which = 4, skip = 1) %>% clean_names()
  suburb_prr_3bdrh <- rio::import(suburb_fileList, which = 5, skip = 1) %>% clean_names()
  suburb_prr_4bdrh <- rio::import(suburb_fileList, which = 6, skip = 1) %>% clean_names()
  suburb_prr_allbdr <- rio::import(suburb_fileList, which = 7, skip = 1) %>% clean_names()
  
  ##############################################
  ### clean the data sheets and combine them ###
  ##############################################
  
  # Use function to clean the files
  lga_rr_1brf <- rr_cleaner(lga_prr_1bdrf, "1 bedroom flat")
  lga_rr_2brf <- rr_cleaner(lga_prr_2bdrf, "2 bedroom flat")
  lga_rr_3brf <- rr_cleaner(lga_prr_3bdrf, "3 bedroom flat")
  lga_rr_2brh <- rr_cleaner(lga_prr_2bdrh, "2 bedroom house")
  lga_rr_3brh <- rr_cleaner(lga_prr_3bdrh, "3 bedroom house")
  lga_rr_4brh <- rr_cleaner(lga_prr_4bdrh, "4 bedroom house")
  lga_rr_tot <- rr_cleaner(lga_prr_allbdr, "All properties")
  
  # Use function to clean the files
  suburb_rr_1brf <- rr_cleaner(suburb_prr_1bdrf, "1 bedroom flat")
  suburb_rr_2brf <- rr_cleaner(suburb_prr_2bdrf, "2 bedroom flat")
  suburb_rr_3brf <- rr_cleaner(suburb_prr_3bdrf, "3 bedroom flat")
  suburb_rr_2brh <- rr_cleaner(suburb_prr_2bdrh, "2 bedroom house")
  suburb_rr_3brh <- rr_cleaner(suburb_prr_3bdrh, "3 bedroom house")
  suburb_rr_4brh <- rr_cleaner(suburb_prr_4bdrh, "4 bedroom house")
  suburb_rr_tot <- rr_cleaner(suburb_prr_allbdr, "All properties")
  
  # Remove the files not needed
  rm(lga_prr_1bdrf, lga_prr_2bdrf, lga_prr_3bdrf, lga_prr_2bdrh, lga_prr_3bdrh, lga_prr_4bdrh, lga_prr_allbdr,
     suburb_prr_1bdrf, suburb_prr_2bdrf, suburb_prr_3bdrf, suburb_prr_2bdrh, suburb_prr_3bdrh, suburb_prr_4bdrh, suburb_prr_allbdr)
  
  # Bind all cleaned excel sheets together
  lga_rr_aff <- bind_rows(lga_rr_1brf, lga_rr_2brf, lga_rr_3brf, lga_rr_2brh, lga_rr_3brh, lga_rr_4brh, lga_rr_tot) 
  
  suburb_rr_aff <- bind_rows(suburb_rr_1brf, suburb_rr_2brf, suburb_rr_3brf, suburb_rr_2brh, suburb_rr_3brh, suburb_rr_4brh, suburb_rr_tot)
  
  # Remove files not needed
  rm(lga_rr_1brf, lga_rr_2brf, lga_rr_3brf, lga_rr_2brh, lga_rr_3brh, lga_rr_4brh, lga_rr_tot,
     suburb_rr_1brf, suburb_rr_2brf, suburb_rr_3brf, suburb_rr_2brh, suburb_rr_3brh, suburb_rr_4brh, suburb_rr_tot)
  
  filedate <- filedate %>% str_to_title() %>%
    str_replace("-", " ")
  
  lga_rr_aff_upload <- lga_rr_aff %>%
    mutate(quarter = str_replace_all(quarter, c("2003_31" = "2002", "2003_39" = "2003" ))) %>%
    filter(area != 'LGA') %>%
    dplyr::rename(lga = area) %>%
    filter(quarter %in% filedate) %>%
    distinct() %>%
    mutate(month_date = mdy(quarter))
  
  suburb_rr_aff_upload <- suburb_rr_aff %>%
    mutate(quarter = str_replace_all(quarter, c("2003_31" = "2002", "2003_39" = "2003"))) %>%
    filter(area != 'Suburb') %>%
    dplyr::rename(suburb = area) %>%
    filter(quarter %in% filedate) %>%
    distinct() %>%
    mutate(month_date = mdy(quarter))
  
  
  #################################################
  ### upload data if it's there are new entries ###
  #################################################
   print("append data")
 
    dbWriteTable(con, SQL("dhhs.lga_rental_reports"), lga_rr_aff_upload, append = TRUE)

    dbWriteTable(con, SQL("dhhs.suburb_rental_reports"), suburb_rr_aff_upload, append = TRUE)
  
  
} #End Else


