#============================================================================================
import pandas as pd
#============================================================================================
# CLEANING THE SA2 TOTAL POPULATION DATASET
#============================================================================================
# Read the SA2 total population by districts dataset

population = pd.read_excel("../generic-buy-now-pay-later-project-group-10-bnpl/data/SA2_total_population/SA2_pop.xlsx",sheet_name="Table 1")
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

nulls = population.isnull().sum()
flag = 0

for val in nulls:

    # If a null value is found, drop the row
    if val > 0:
        population.dropna(inplace=True)
        flag = 1
#--------------------------------------------------------------------------------------------
# Save the final curated dataset as csv file

population.to_csv("../generic-buy-now-pay-later-project-group-10-bnpl/data/curated/SA2_total_population.csv")
#--------------------------------------------------------------------------------------------