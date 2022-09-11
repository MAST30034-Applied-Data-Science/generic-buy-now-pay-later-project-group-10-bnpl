from urllib.request import urlretrieve

#--------------------------------------------------------------------------------------------
# Downloading the covid data 
url = "https://raw.githubusercontent.com/M3IT/COVID-19_Data/master/Data/COVID_AU_state_daily_change.csv"
# Define the file names
output_csv = "../generic-buy-now-pay-later-project-group-10-bnpl/data/covid.csv"
# Download the data
urlretrieve(url, output_csv) 
#--------------------------------------------------------------------------------------------

