# Importing required libraries
from urllib.request import urlretrieve
from urllib.request import urlretrieve
from zipfile import ZipFile
import os

#============================================================================================
# COVID DATA DOWNLOAD
#============================================================================================
# Specify the url
url = "https://raw.githubusercontent.com/M3IT/COVID-19_Data/master/Data/COVID_AU_state_daily_change.csv"

#--------------------------------------------------------------------------------------------
# Define the file names
output_csv = "../data/covid.csv"

#--------------------------------------------------------------------------------------------
# Download the data
urlretrieve(url, output_csv) 

#============================================================================================
# MAKE NEW DIRECTORIES TO SAVE THE ABS DATASETS
#============================================================================================
# Code adapted from MAST30034 Tutorial 1
# from the current `tute_1` directory, go back two levels to the `MAST30034` directory
output_relative_dir = '../data/'

#--------------------------------------------------------------------------------------------
# check if it exists as it makedir will raise an error if it does exist
if not os.path.exists(output_relative_dir):
    os.makedirs(output_relative_dir)

#--------------------------------------------------------------------------------------------
# Define the directory names
dirs = ['SA2_boundaries', 'SA2_total_population', 'SA2_income', 'SA2_census']

#--------------------------------------------------------------------------------------------
# now, for each type of data set we will need, we will create the paths
for target_dir in dirs: # taxi_zones should already exist
    if not os.path.exists(output_relative_dir + target_dir):
        os.makedirs(output_relative_dir + target_dir)

#============================================================================================
# SA2 BOUNDARIES DATASET DOWNLOAD
#============================================================================================

# Specify the url
SA2_URL_ZIP = "https://www.abs.gov.au/statistics/standards/australian-statistical-geography-standard-asgs-edition-3/jul2021-jun2026/access-and-downloads/digital-boundary-files/SA2_2021_AUST_SHP_GDA2020.zip"

#--------------------------------------------------------------------------------------------
# Define the file names
output_zip = "../data/SA2_boundaries/SA2.zip"

#--------------------------------------------------------------------------------------------
# Download the data
urlretrieve(SA2_URL_ZIP, output_zip)

#--------------------------------------------------------------------------------------------
# Extracting the zip file of the geospatial data
# Specifythe zip file name
file_name = "../data/SA2_boundaries/SA2.zip"
  
#--------------------------------------------------------------------------------------------
# opening the zip file in READ mode
with ZipFile(file_name, 'r') as zip:
    # extracting all the files
    zip.extractall(path = "../data/SA2_boundaries/")

#============================================================================================
# SA2 TOTAL POPULATION DATASET DOWNLOAD
#============================================================================================
# Specify the url
SA2_URL_POP = "https://www.abs.gov.au/statistics/people/population/regional-population/2021/32180DS0001_2001-21.xlsx"

#--------------------------------------------------------------------------------------------
# Define the file names
output = "../data/SA2_total_population/SA2_pop.xlsx"

#--------------------------------------------------------------------------------------------
# Download the data
urlretrieve(SA2_URL_POP, output)

#============================================================================================
# SA2 INCOME DATA DOWNLOAD
#============================================================================================
# Specify the url
SA2_URL_INCOME = "https://www.abs.gov.au/statistics/labour/earnings-and-working-conditions/personal-income-australia/2014-15-2018-19/6524055002_DO001.xlsx"

#--------------------------------------------------------------------------------------------
# Define the file names
output = "../data/SA2_income/SA2_income.xlsx"

#--------------------------------------------------------------------------------------------
# Download the data
urlretrieve(SA2_URL_INCOME, output)

#============================================================================================
# SA2 CENSUS DATA DOWNLOAD
#============================================================================================
# Specify the url
SA2_CENSUS_URL = "https://www.abs.gov.au/census/find-census-data/datapacks/download/2021_GCP_SA2_for_AUS_short-header.zip"

#--------------------------------------------------------------------------------------------
# Define the file name
output_csv = "../data/SA2_census/census.zip"

#--------------------------------------------------------------------------------------------
# Download the data
urlretrieve(SA2_CENSUS_URL, output_csv) 

#--------------------------------------------------------------------------------------------
# Opening the zip file in read mode
with ZipFile(output_csv, 'r') as zip:
    # extracting all the files
    zip.extractall(path = "../data/SA2_census/")
#============================================================================================
# SA2 TO POSTCODE DATA DOWNLOAD
#============================================================================================
# Specify the url
SA2_POSTCODE_URL = "https://raw.githubusercontent.com/matthewproctor/australianpostcodes/master/australian_postcodes.csv"

#--------------------------------------------------------------------------------------------
# Define the file names
output_csv = "../data/curated/postcode.csv"

#--------------------------------------------------------------------------------------------
# Download the data
urlretrieve(SA2_POSTCODE_URL, output_csv) 
