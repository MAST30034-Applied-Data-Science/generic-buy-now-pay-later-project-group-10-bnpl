#==============================================================================
import pandas as pd
#==============================================================================
# LOAD THE DATA
#==============================================================================
# Retrive the required data
population = pd.read_csv("../data/curated/SA2_total_population.csv")
income = pd.read_csv("../data/curated/SA2_income.csv")
census = pd.read_csv("../data/curated/SA2_census.csv")
boundaries = pd.read_csv("../data/curated/SA2_district_boundaries.csv")

# -----------------------------------------------------------------------------
# Remove the first unwanted column
population = population.iloc[:,1:]
income = income.iloc[:,1:]
census = census.iloc[:,1:]
boundaries = boundaries.iloc[:,1:]

# -----------------------------------------------------------------------------
# Perform inner join on income and census dataset
income_census = pd.merge(income, census, on = 'SA2_code')

# -----------------------------------------------------------------------------
# Perform inner join on population and boundaries dataset
pop_bd = pd.merge(population, boundaries, on = ['SA2_code','SA2_name', 
'state_code', 'state_name'])

# -----------------------------------------------------------------------------
# Make a final dataset from the the merged datasets
SA2_datasets = pd.merge(income_census, pop_bd, on = ['SA2_code', 'SA2_name'])

# -----------------------------------------------------------------------------
# Save the final dataset
SA2_datasets.to_csv("../data/curated/merged_SA2_data.csv")