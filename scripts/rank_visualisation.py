#This script visualise the final ranking output for the top 100 merchants.

import pandas as pd


# top 100 overall
rank = pd.read_csv("../data/curated/final_rank.csv").drop(columns="Unnamed: 0.1")
rank.columns = rank.columns.str.replace("Unnamed: 0", "rank")
rank["total_revenue (in hundred)"] = rank["total_revenue"] / 100


# rank for each categories
# category for the merchants
category_labels = rank.groupby(by="category").count().index

# rank for each category contained in the top 100 merchants result
rank_category = [rank[rank['category'] == cat] for cat in category_labels]


# Top 5 merchants 
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


# Top 5 merchants in "Beauty, Health, Personal and Household"
# make sure it is ordered by the rank 
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


# Top 5 merchants in "Books, Stationary and Music"
# make sure it is ordered by the rank 
data = rank_category[1].sort_values("rank").reset_index(drop=True)

# category name
category = data["category"][0]

# calculate the average
avg_rank = data.loc[:,["total_revenue (in hundred)", "total_future_customers", "total_earnings_of_BNPL", "total_future_transactions", "average_fraud_rate_per_merchant"]].mean()
avg_rank["merchant_name"] = f"average of merchants in\n {category}"

data = data.loc[[4,3,2,1,0],["merchant_name", "total_revenue (in hundred)", "total_future_customers", "total_earnings_of_BNPL", "total_future_transactions", "average_fraud_rate_per_merchant"]]

# add average of merchants with category "Books, Stationary and Music" with rank above 100
data = data.append(avg_rank, ignore_index = True)

# label by merchant name
data = data.rename(columns={"merchant_name": "merchant"}).set_index("merchant")

data.plot.barh(title=f"Top 5 merchants in {category}").legend(bbox_to_anchor=(1.01, 1), loc='upper left', borderaxespad=0)


# Top 5 merchants in "Electronics"
# make sure it is ordered by the rank 
data = rank_category[2].sort_values("rank").reset_index(drop=True)

# category name
category = data["category"][0]

# calculate the average
avg_rank = data.loc[:,["total_revenue (in hundred)", "total_future_customers", "total_earnings_of_BNPL", "total_future_transactions", "average_fraud_rate_per_merchant"]].mean()
avg_rank["merchant_name"] = f"average of merchants in\n {category}"

data = data.loc[[4,3,2,1,0],["merchant_name", "total_revenue (in hundred)", "total_future_customers", "total_earnings_of_BNPL", "total_future_transactions", "average_fraud_rate_per_merchant"]]

# add average of merchants with category "Electronics" with rank above 100
data = data.append(avg_rank, ignore_index = True)

# label by merchant name
data = data.rename(columns={"merchant_name": "merchant"}).set_index("merchant")

data.plot.barh(title=f"Top 5 merchants in {category}").legend(bbox_to_anchor=(1.01, 1), loc='upper left', borderaxespad=0)


# Top 5 merchants in "Furniture"
# make sure it is ordered by the rank 
data = rank_category[3].sort_values("rank").reset_index(drop=True)

# category name
category = data["category"][0]

# calculate the average
avg_rank = data.loc[:,["total_revenue (in hundred)", "total_future_customers", "total_earnings_of_BNPL", "total_future_transactions", "average_fraud_rate_per_merchant"]].mean()
avg_rank["merchant_name"] = f"average of merchants in\n {category}"

data = data.loc[[4,3,2,1,0],["merchant_name", "total_revenue (in hundred)", "total_future_customers", "total_earnings_of_BNPL", "total_future_transactions", "average_fraud_rate_per_merchant"]]

# add average of merchants with category "Furniture" with rank above 100
data = data.append(avg_rank, ignore_index = True)

# label by merchant name
data = data.rename(columns={"merchant_name": "merchant"}).set_index("merchant")

data.plot.barh(title=f"Top 5 merchants in {category}").legend(bbox_to_anchor=(1.01, 1), loc='upper left', borderaxespad=0)


# Top 5 merchants in "Toys and DIY"
# make sure it is ordered by the rank 
data = rank_category[4].sort_values("rank").reset_index(drop=True)

# category name
category = data["category"][0]

# calculate the average
avg_rank = data.loc[:,["total_revenue (in hundred)", "total_future_customers", "total_earnings_of_BNPL", "total_future_transactions", "average_fraud_rate_per_merchant"]].mean()
avg_rank["merchant_name"] = f"average of merchants in\n {category}"

data = data.loc[[4,3,2,1,0],["merchant_name", "total_revenue (in hundred)", "total_future_customers", "total_earnings_of_BNPL", "total_future_transactions", "average_fraud_rate_per_merchant"]]

# add average of merchants with category "Toys and DIY" with rank above 100
data = data.append(avg_rank, ignore_index = True)

# label by merchant name
data = data.rename(columns={"merchant_name": "merchant"}).set_index("merchant")

data.plot.barh(title=f"Top 5 merchants in {category}").legend(bbox_to_anchor=(1.01, 1), loc='upper left', borderaxespad=0)