#============================================================================================
# Importing required libraries
import sys
import pandas as pd

#--------------------------------------------------------------------------------------------
# take list of values as weights for fraud, transactions, revenue, customer, take rate
weights = list(sys.argv[1].split(','))
#--------------------------------------------------------------------------------------------
# reading in data
# fraud = pd.read_parquet("../data/curated/fraud_feature.parquet")
transactions = pd.read_csv("../data/curated/no_cust_ranking_feature.csv")
tags = pd.read_csv("../data/curated/tagged_merchants.csv")
# take_rate = pd.read_csv("../data/curated/BNPL_earnings.csv")
revenue = pd.read_csv("../data/curated/revenue.csv")
customers = pd.read_csv("../data/curated/customers.csv")

# adjusting columns
customers = customers[['merchant_name', 'total_future_customers']]
revenue = revenue[['merchant_name', 'total_revenue']]
tags = tags[['name', 'merchant_abn', 'category']]
tags = tags.rename(columns={'name': 'merchant_name'})

# fraud = fraud.rename(columns={'merchant_abn_1': 'merchant_abn', 'average fraud rate per merchant': 'fraud'})
# fraud['merchant_abn'] = fraud['merchant_abn'].astype('int')

tags_trans = pd.merge(tags, transactions, on=['merchant_abn','merchant_name'], how='inner')
add_customers = pd.merge(tags_trans, customers, on ='merchant_name', how='inner')
add_revenue = pd.merge(add_customers, revenue, on ='merchant_name', how='inner')

# fraud_trans_tags = pd.merge(fraud_trans, tags, on=['merchant_abn', 'merchant_name'], how='inner')


#--------------------------------------------------------------------------------------------
# adding in the weights
fraud_weights = int(weights[0])
transactions_weights = int(weights[1])
revenue_weights = int(weights[2])
customer_weights = int(weights[3])
take_rate_weights = int(weights[4])

tags_trans['ranking_feature'] = transactions_weights*add_revenue['prediction'] + \
                                revenue_weights*add_revenue['total_revenue'] + \
                                customer_weights*add_revenue['total_future_customers']

# print(fraud_trans_tags.head(5))

#--------------------------------------------------------------------------------------------
# splitting by tags for top 10 merchants
tags = tags_trans.category.unique()

for tag in tags:
    print("Ranking for ", tag, "category: ")
    df = tags_trans.query("category == @tag")
    # print("Number of merchants in this category: ",len(df))
    
    df = df.sort_values(by='ranking_feature', ascending=False)
    merchant_rank = df['merchant_name'].reset_index(drop = True)
    
    for i in range(10):
        print("Rank ", i+1, ": ", merchant_rank[i])
    print("\n")
    
#--------------------------------------------------------------------------------------------
# top 100 merchants
final_rank = tags_trans.sort_values(by='ranking_feature', ascending=False)
final_rank = final_rank.head(100)
final_rank = final_rank[['merchant_name', 'ranking_feature']]
final_rank = final_rank.reset_index(drop = True)

final_rank.to_csv("../data/curated/final_rank.csv", index = True)
