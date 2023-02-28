#------------------------------------------------------------------------------
# PROJECT NAME/FOLDER: RScripts/DataProcessing/Covid19Vaccinations
# FILENAME: jobseeker_monthly_dl.R
# AUTHOR: Nhung Seidensticker
# R VERSION: 4.0.2
# DATE CREATED: 15.11.21
# DESCRIPTION: Script for download of jobseeker data
#------------------------------------------------------------------------------

library(rvest)
library(tidyverse)
library(readxl)
library(janitor)
library(lubridate)
library(httr)
library(dbplyr)
library(glue)
library(zoo)

# database table ----------------------------------------------------------------------------------

WRITE_DATA_NAME <- "jobseeker_monthly_payments_sa2"

# Find latest date in the database
current_data <- tbl(con, in_schema("dss", WRITE_DATA_NAME)) %>% collect() 
latest_date <- current_data %>%
  mutate(month = mdy(month))


max_date <- max(latest_date$month) %>%
  format("%B-%Y") %>%
  tolower()

# data source
url <- "https://data.gov.au/data/dataset/jobseeker-payment-and-youth-allowance-recipients-monthly-profile"

site <- read_html(url)

# Get the list of files (saves it as one character)
fileList <- site %>% 
  html_nodes("a") %>% # find all links on the page
  html_attr("href") %>% # get the urls of those links
  str_subset("\\.xlsx") # keep only those links that are xlsx files

# Change the list to an actual list class
fileList <- as.list(strsplit(fileList, " "))

# Check the latest record against previous record
latest_source <- fileList[[1]]

if (grepl(max_date, latest_source, fixed = TRUE)) {
  
  print("Data already in the database")
  
  # Remove files that are no longer needed
  rm(fileList, site, current_data, latest_date)
  
} else {
    jobseeker <- rio::import(latest_source,
                           which = "Table 4 - By SA2", 
                           skip = 6) %>%
    
                  filter(!is.na(SA2)) %>%
                
                  mutate(across(c("JobSeeker Payment",
                                "Youth Allowance (other)"),
                              as.numeric),
                              Source = latest_source)
    
    pattern <- "((january|february|march|april|may|june|july|august|september|october|november|december)-\\d\\d\\d\\d)"
    
    # Create the final file
    jobseeker_append <- jobseeker %>%
      dplyr::rename(sa2_5dig = `SA2`,
                    sa2_name = `SA2 Name`,
                    `Youth Allowance Payments` = `Youth Allowance (other)`) %>%
      # Apply regex to extract the month-year
      mutate(Month = str_to_sentence(str_extract(Source, pattern))) %>%
      mutate(Month = str_replace_all(Month, '-', ' ')) %>%
      dplyr::select(-Source) %>%
      # Pivot longer for tidy data
      pivot_longer(-c(sa2_5dig, sa2_name, Month), names_to = "series", values_to = "value") %>%
      dplyr::rename_all(tolower) %>%
      mutate(month_date = mdy(quarter))

    
    # Remove files that are no longer needed
    rm(fileList, site)
    
    print("append")
    
    # append new data to the datbase
    dbWriteTable(con, SQL("dss.jobseeker_monthly_payments_sa2"), jobseeker_append, append = TRUE)
}

