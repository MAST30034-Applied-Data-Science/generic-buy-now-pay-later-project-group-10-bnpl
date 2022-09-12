from urllib.request import urlretrieve
from urllib.request import urlretrieve
from zipfile import ZipFile
import os

#--------------------------------------------------------------------------------------------

# Downloading the covid data 
url = "https://raw.githubusercontent.com/M3IT/COVID-19_Data/master/Data/COVID_AU_state_daily_change.csv"
# Define the file names
output_csv = "../generic-buy-now-pay-later-project-group-10-bnpl/data/covid.csv"
# Download the data
urlretrieve(url, output_csv) 

#============================================================================================
# MAKE NEW DIRECTORIES TO SAVE THE ABS DATASETS
#============================================================================================
# Code adapted from MAST30034 Tutorial 1
# from the current `tute_1` directory, go back two levels to the `MAST30034` directory
output_relative_dir = '../data/'

# check if it exists as it makedir will raise an error if it does exist
if not os.path.exists(output_relative_dir):
    os.makedirs(output_relative_dir)

# Define the directory names
dirs = ['SA2_boundaries', 'SA2_total_population', 'SA2_income', 'SA2_census']

# now, for each type of data set we will need, we will create the paths
for target_dir in dirs: # taxi_zones should already exist
    if not os.path.exists(output_relative_dir + target_dir):
        os.makedirs(output_relative_dir + target_dir)

#--------------------------------------------------------------------------------------------

# Download the SA2 boundaries dataset

SA2_URL_ZIP = "https://www.abs.gov.au/statistics/standards/australian-statistical-geography-standard-asgs-edition-3/jul2021-jun2026/access-and-downloads/digital-boundary-files/SA2_2021_AUST_SHP_GDA2020.zip"

# Define the file names
output_zip = "../generic-buy-now-pay-later-project-group-10-bnpl/data/SA2_boundaries/SA2.zip"

# Download the data
urlretrieve(SA2_URL_ZIP, output_zip)

# Extracting the zip file of the geospatial data

# specifying the zip file name
file_name = "../generic-buy-now-pay-later-project-group-10-bnpl/data/SA2_boundaries/SA2.zip"
  
# opening the zip file in READ mode
with ZipFile(file_name, 'r') as zip:
    # extracting all the files
    zip.extractall(path = "../generic-buy-now-pay-later-project-group-10-bnpl/data/SA2_boundaries/")

#--------------------------------------------------------------------------------------------
# Download the SA2 boundaries dataset

SA2_URL_pop = "https://www.abs.gov.au/statistics/people/population/regional-population/2021/32180DS0001_2001-21.xlsx"

# Define the file names
output = "../generic-buy-now-pay-later-project-group-10-bnpl/data/SA2_total_population/SA2_pop.xlsx"

# Download the data
urlretrieve(SA2_URL_pop, output)


