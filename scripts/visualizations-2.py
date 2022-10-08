
#==============================================================================
import matplotlib.pyplot as plt
import outlier
import pandas as pd
import seaborn as sns
import folium
from pyspark.sql.functions import date_format
from pyspark.sql import SparkSession, functions as F

import geopandas as gpd

#==============================================================================

# top 100 overall
rank = pd.read_csv("../data/curated/final_rank.csv").drop(columns="Unnamed: 0.1")
rank.columns = rank.columns.str.replace("Unnamed: 0", "rank")
rank["total_revenue (in hundred)"] = rank["total_revenue"] / 100
# rank for each categories
# category for the merchants
category_labels = rank.groupby(by="category").count().index

# rank for each category contained in the top 100 merchants result
rank_category = [rank[rank['category'] == cat] for cat in category_labels]
# maybe check the past transaction, revenue for a partcular merchant if necessary?
rank.columns
# calculate the average
avg_rank = rank.loc[:,["total_revenue (in hundred)", "total_future_customers", "total_earnings_of_BNPL", "total_future_transactions", "average_fraud_rate_per_merchant"]].mean()
avg_rank["merchant_name"] = "average of top 100"

# 4, 3, 2, 1, 0
# get top 5 merchants
data = rank.loc[[4,3,2,1,0],["merchant_name", "total_revenue (in hundred)", "total_future_customers", "total_earnings_of_BNPL", "total_future_transactions", "average_fraud_rate_per_merchant"]]

# add average of top 100 merchants for comparison
data = data.append(avg_rank, ignore_index = True)

# label by merchant name
data = data.rename(columns={"merchant_name": "merchant"}).set_index("merchant")
data.plot.barh(title=f"Top 5 merchants").legend(bbox_to_anchor=(1.01, 1), loc='upper left', borderaxespad=0)
#rank_category[0].sort_values("rank").reset_index(drop=True)
# make sure it is ordered byt the rank 
data = rank_category[0].sort_values("rank").reset_index(drop=True)

# category name
category = data["category"][0]

# calculate the average
avg_rank = data.loc[:,["total_revenue (in hundred)", "total_future_customers", "total_earnings_of_BNPL", "total_future_transactions", "average_fraud_rate_per_merchant"]].mean()
avg_rank["merchant_name"] = f"average of merchants in\n {category}"

# top 5 wihtin the category
data = data.loc[[4,3,2,1,0],["merchant_name", "total_revenue (in hundred)", "total_future_customers", "total_earnings_of_BNPL", "total_future_transactions", "average_fraud_rate_per_merchant"]]

# add average of merchants with category "Beauty, Health, Personal and Household" with rank above 100
data = data.append(avg_rank, ignore_index = True)

# label by merchant name
data = data.rename(columns={"merchant_name": "merchant"}).set_index("merchant")

data.plot.barh(title=f"Top 5 merchants in {category}").legend(bbox_to_anchor=(1.01, 1), loc='upper left', borderaxespad=0)
# make sure it is ordered byt the rank 
data = rank_category[1].sort_values("rank").reset_index(drop=True)

# category name
category = data["category"][0]

# calculate the average
avg_rank = data.loc[:,["total_revenue (in hundred)", "total_future_customers", "total_earnings_of_BNPL", "total_future_transactions", "average_fraud_rate_per_merchant"]].mean()
avg_rank["merchant_name"] = f"average of merchants in\n {category}"


data = data.loc[[4,3,2,1,0],["merchant_name", "total_revenue (in hundred)", "total_future_customers", "total_earnings_of_BNPL", "total_future_transactions", "average_fraud_rate_per_merchant"]]

# add average of merchants with category "Beauty, Health, Personal and Household" with rank above 100
data = data.append(avg_rank, ignore_index = True)

# label by merchant name
data = data.rename(columns={"merchant_name": "merchant"}).set_index("merchant")

data.plot.barh(title=f"Top 5 merchants in {category}").legend(bbox_to_anchor=(1.01, 1), loc='upper left', borderaxespad=0)
# make sure it is ordered byt the rank 
data = rank_category[2].sort_values("rank").reset_index(drop=True)

# category name
category = data["category"][0]

# calculate the average
avg_rank = data.loc[:,["total_revenue (in hundred)", "total_future_customers", "total_earnings_of_BNPL", "total_future_transactions", "average_fraud_rate_per_merchant"]].mean()
avg_rank["merchant_name"] = f"average of merchants in\n {category}"

data = data.loc[[4,3,2,1,0],["merchant_name", "total_revenue (in hundred)", "total_future_customers", "total_earnings_of_BNPL", "total_future_transactions", "average_fraud_rate_per_merchant"]]

# add average of merchants with category "Beauty, Health, Personal and Household" with rank above 100
data = data.append(avg_rank, ignore_index = True)

# label by merchant name
data = data.rename(columns={"merchant_name": "merchant"}).set_index("merchant")

data.plot.barh(title=f"Top 5 merchants in {category}").legend(bbox_to_anchor=(1.01, 1), loc='upper left', borderaxespad=0)
# make sure it is ordered byt the rank 
data = rank_category[3].sort_values("rank").reset_index(drop=True)

# category name
category = data["category"][0]

# calculate the average
avg_rank = data.loc[:,["total_revenue (in hundred)", "total_future_customers", "total_earnings_of_BNPL", "total_future_transactions", "average_fraud_rate_per_merchant"]].mean()
avg_rank["merchant_name"] = f"average of merchants in\n {category}"

data = data.loc[[4,3,2,1,0],["merchant_name", "total_revenue (in hundred)", "total_future_customers", "total_earnings_of_BNPL", "total_future_transactions", "average_fraud_rate_per_merchant"]]

# add average of merchants with category "Beauty, Health, Personal and Household" with rank above 100
data = data.append(avg_rank, ignore_index = True)

# label by merchant name
data = data.rename(columns={"merchant_name": "merchant"}).set_index("merchant")

data.plot.barh(title=f"Top 5 merchants in {category}")
# make sure it is ordered by the rank 
data = rank_category[4].sort_values("rank").reset_index(drop=True)

# category name
category = data["category"][0]

# calculate the average
avg_rank = data.loc[:,["total_revenue (in hundred)", "total_future_customers", "total_earnings_of_BNPL", "total_future_transactions", "average_fraud_rate_per_merchant"]].mean()
avg_rank["merchant_name"] = f"average of merchants in\n {category}"

data = data.loc[[4,3,2,1,0],["merchant_name", "total_revenue (in hundred)", "total_future_customers", "total_earnings_of_BNPL", "total_future_transactions", "average_fraud_rate_per_merchant"]]

# add average of merchants with category "Beauty, Health, Personal and Household" with rank above 100
data = data.append(avg_rank, ignore_index = True)

# label by merchant name
data = data.rename(columns={"merchant_name": "merchant"}).set_index("merchant")

data.plot.barh(title=f"Top 5 merchants in {category}")
