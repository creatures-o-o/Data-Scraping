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


################################################
### DSS Payment Demographic data by Postcode ###
################################################

### About the data ###
# The source is https://data.gov.au/data/dataset/dss-payment-demographic-data
# The data contains the number of DSS payment type by month for postcodses from September 2013 onwards.
# The clean data contains the following columns:
# SA2_Code: The SA2 5-digit code (ASGS)
# SA2_Name: The SA2 name
# Month: A character string column to preserve month-year. Lubridate would convert to dmy, and zoo objects (other time series) don't fit in postgres.
# Series: Character string. Either 'JobSeeker Payments' or 'Youth Allowance Payments'
# Value: Numeric. The number associated with the observation

# Note that NAs mean there are fewer than 5 JobKeeper applications in the Postcode.

########################################
### set global values for write data ###
########################################

POA_WRITE_DATA_NAME <- "payment_types_qrt_pc"
LGA_WRITE_DATA_NAME <- "payment_types_qrt_lga"
SA2_WRITE_DATA_NAME <- "payment_types_qrt_sa2"

# define the pattern for month matching
PATTERN <- "((january|february|march|april|may|june|july|august|september|october|november|december|dec|sept)-\\d\\d\\d\\d)|((january|february|march|april|may|june|july|august|september|october|november|december|dec|sept)\\d\\d\\d\\d)|201509"

######################################################################
### get the data and find the most recent date in the current data ###
######################################################################

# Get the URL
url <- "https://data.gov.au/data/dataset/dss-payment-demographic-data"

site <- read_html(url)

# Get the list of files (saves it as one character)
fileList <- site %>% 
  html_nodes("#dataset-resources a") %>% # find all links on the page
  html_attr("href") %>% # get the urls of those links
  str_subset("https://") # keep only those links that are xlsx files

# Change the list to an actual list class
fileList <- as.list(strsplit(fileList, " "))

latest_source <- fileList[[1]]

# download the latest file into temp 
temp <- tempfile()
download.file(latest_source, temp)

# get a list of sheet names
sheets <- excel_sheets(temp)

# read in the data and perform preliminary clean

sa2_dss <- read_excel(temp, sheet = sheets[grepl("SA", sheets)], skip = 2) %>%
  filter(!is.na(SA2)) 

lga_dss <- read_excel(temp, sheet = sheets[grepl("LGA", sheets)], skip = 2) %>%
  filter(!is.na(LGA))

poa_dss <- read_excel(temp, sheet = sheets[grepl("Post", sheets)], skip = 2) %>%
  filter(!is.na(Postcode)) 



# Find latest date in the database
# CON <- dbCon() # connect via all file

current_data <- tbl(con, in_schema("dss", POA_WRITE_DATA_NAME)) %>% collect() 
latest_date <- current_data %>%
  mutate(quarter = mdy(quarter))

max_date <- max(latest_date$quarter) %>%
  format("%B-%Y") %>%
  tolower()


#########################################
### Create function to clean the data ###
#########################################

clean_dss <- function(file_source){
  
  dss_data <-  file_source %>%
    mutate_all(str_replace_all,"<", "") %>%
    mutate_at(vars(`Commonwealth Seniors Health Card`), as.numeric) %>%
    mutate_at(vars(`Health Care Card`), as.numeric) %>%
    mutate(source = latest_source) %>%
    
    # Apply regex to extract the month-year
    mutate(quarter = str_to_sentence(str_extract(source, PATTERN))) %>%
    
    dplyr::select(-source) %>%
    
    #remove characters and convert to numeric
    mutate_all(str_replace_all,"<", "") %>%
    
    #mutate(across(-c("Postcode", "Month"), as.numeric)) %>%
    mutate_if(., is.numeric, ~replace(., is.na(.), 0))  %>%
    
    
    mutate(quarter = str_replace_all(quarter, c("Dec-2019" = 'December-2019', "Sept-2019" = 'September-2019', 
                                            "201509" = 'September-2015', "December2013" = "December-2013", 
                                            "December2014" = "December-2014", "March2014" = "March-2014",
                                            "September2013" = "September-2013", "September2014" = "September-2014")))
  
}




#########################################################################
### determine if the data needs updating, and updates db if necessary ###
#########################################################################

if (grepl(max_date, latest_source, fixed = TRUE)) {
  
  print("Data already in the database")
  
  # Remove files that are no longer needed
  rm(fileList, site, current_data, latest_date)
  
} else {
  
  #########################################
  ### Prepare final data to be uploaded ###
  #########################################
  
  sa2_dss_data <- clean_dss(sa2_dss) %>% 
    filter(str_detect(SA2, "^2")) %>%
    # Pivot longer for tidy data
    pivot_longer(-c(SA2, `SA2 Name`, quarter), names_to = "series", values_to = "value") %>%
    #remove records with 0
    filter_at(vars(value), all_vars(.>0)) %>%
    dplyr::rename(sa2_5dig = SA2, sa2_name = `SA2 Name`) %>%
    #rename columns to lower case to comply with Postgres
    dplyr::rename_all(tolower) %>%
    mutate_at(vars(value), as.numeric) %>%
    mutate(month_date = mdy(quarter))
  
  lga_dss_data <- clean_dss(lga_dss) %>% 
    filter(str_detect(LGA, "^2")) %>%
    # Pivot longer for tidy data
    pivot_longer(-c(LGA, `LGA name`, quarter), names_to = "series", values_to = "value") %>%
    #remove records with 0
    filter_at(vars(value), all_vars(.>0)) %>%
    dplyr::rename(lga_code = LGA, lga_name = `LGA name`) %>%
    #rename columns to lower case to comply with Postgres
    dplyr::rename_all(tolower) %>%
    mutate_at(vars(value), as.numeric) %>%
    mutate(month_date = mdy(quarter))
  
  poa_dss_data <- clean_dss(poa_dss) %>% 
    filter(str_detect(Postcode, "^3")) %>%
    # Pivot longer for tidy data
    pivot_longer(-c(Postcode, quarter), names_to = "series", values_to = "value") %>%
    #remove records with 0
    filter_at(vars(value), all_vars(.>0)) %>%
    #rename columns to lower case to comply with Postgres
    dplyr::rename_all(tolower) %>%
    mutate_at(vars(value), as.numeric) %>%
    mutate(month_date = mdy(quarter))
  
  
  dbWriteTable(con, SQL("dss.{SA2_WRITE_DATA_NAME}"), sa2_dss_data, append = TRUE)
  dbWriteTable(con, SQL("dss.{LGA_WRITE_DATA_NAME}"), lga_dss_data, append = TRUE)
  dbWriteTable(con, SQL("dss.{POA_WRITE_DATA_NAME}"), poa_dss_data, append = TRUE)

  
}


#----------------------------------------------------------------------------



