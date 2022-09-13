#============================================================================================
import pandas as pd
import geopandas as gpd

#============================================================================================
# CLEANING THE SA2 TOTAL POPULATION DATASET
#============================================================================================
# Read the SA2 total population by districts dataset

population = pd.read_excel("../data/SA2_total_population/SA2_pop.xlsx",sheet_name="Table 1")
#--------------------------------------------------------------------------------------------
# Select the relevant rows to filter out uneccessary data

population = population.iloc[8:,:31]
#--------------------------------------------------------------------------------------------
# Define the new column names for better readability

cols = ['S/T_code', 'S/T_name', 'GCCSA_code', 'GCCSA_name', 'SA4_code', 'SA4_name',
'SA3_code', 'SA3_name', 'SA2_code','SA2_name', '2001', '2002', '2003','2004','2005','2006',
'2007','2008','2009','2010','2011','2012','2013','2014','2015','2016','2017','2018','2019',
'2020','2021']	
#--------------------------------------------------------------------------------------------
# Set the new column names to the dataframe

population.columns = cols
#--------------------------------------------------------------------------------------------
# Filter out the unwanted rows

population.dropna(subset=['S/T_code'], inplace=True)
population.drop([2466, 2468], inplace=True)
#--------------------------------------------------------------------------------------------
# Checking for null values

population.dropna(inplace=True)
#--------------------------------------------------------------------------------------------
# Save the final curated dataset as csv file

population.to_csv("../data/curated/SA2_total_population.csv")

#============================================================================================
# CLEANING THE SA2 DISTRICT BOUNDARIES DATASET
#============================================================================================

# Loading the data
boundaries = gpd.read_file("../data/SA2_boundaries/SA2_2021_AUST_GDA2020.shp")
#--------------------------------------------------------------------------------------------
# Checking for null values

boundaries.dropna(inplace=True)

#--------------------------------------------------------------------------------------------
# Converting the geometrical objects to latitude and longitude for geospatial visualizations

boundaries['geometry'] = boundaries['geometry'].to_crs("+proj=longlat +ellps=WGS84 +datum=WGS84 +no_defs")
#--------------------------------------------------------------------------------------------
# Saving the cleaned dataset

boundaries.to_csv("../data/curated/SA2_district_boundaries.csv")

#============================================================================================
# CLEANING THE SA2 INCOME DATASET
#============================================================================================

# Loading the data
income = pd.read_excel("../data/SA2_income/SA2_income.xlsx", sheet_name="Table 1.4")

#--------------------------------------------------------------------------------------------
# Select the necessary columns

income = income.iloc[6:,[0,1,12,13,14,15,16]]
#--------------------------------------------------------------------------------------------
# Define the new column names for better readability
cols_income = ['SA2', "SA2_name", "2014-2015", "2015-2016", "2016-2017", "2017-2018", "2018-2019"]

#--------------------------------------------------------------------------------------------
# Set the new column names to the dataframe
income.columns = cols_income

#--------------------------------------------------------------------------------------------
# Remove the unwanted values i.e. values that are of type string in the numeric columns 

for index, rows in income.iteritems():
    if index != 'SA2_name':
        for value in rows.values:
            if type(value) == str: 
                income = income[income[index] != value]
#--------------------------------------------------------------------------------------------
# Checking for null values

income.dropna(inplace=True)
#--------------------------------------------------------------------------------------------
# Saving the cleaned dataset

income.to_csv("../data/curated/SA2_income.csv")
        
#============================================================================================
# CLEANING THE SA2 INCOME DATASET
#============================================================================================
# Read the csv file
census = pd.read_csv("../data/SA2_census/2021 Census GCP Statistical Area 2 for AUS/2021Census_G01_AUST_SA2.csv")

#--------------------------------------------------------------------------------------------
# Drop the null values
census.dropna()

#--------------------------------------------------------------------------------------------
# Save as a csv file
census.to_csv("../data/curated/SA2_census.csv")

#============================================================================================
# CLEANING THE COVID-19 dataset
#============================================================================================
# Read the data
covid = pd.read_csv("../data/covid.csv")

#--------------------------------------------------------------------------------------------
# Extract the year, month and date from the timestamp
covid['yyyy'] = pd.to_datetime(covid['date']).dt.year
covid['mm'] = pd.to_datetime(covid['date']).dt.month
covid['dd'] = pd.to_datetime(covid['date']).dt.day

#--------------------------------------------------------------------------------------------
# Save the cleaned data to the curated folder
covid.to_csv("../data/curated/covid.csv")

#--------------------------------------------------------------------------------------------