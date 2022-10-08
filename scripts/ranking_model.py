#==============================================================================
# Importing required libraries
import sys
import pandas as pd

#------------------------------------------------------------------------------
# take list of values as weights for fraud, transactions, revenue, customer, 
# bnpl earnings
weights = list(sys.argv[1].split(','))

#==============================================================================
# READING IN DATA
#==============================================================================
fraud = pd.read_parquet("../data/tables/avg_fraud_rate_per_merchant.parquet")
transactions = pd.read_csv("../data/curated/no_cust_ranking_feature.csv")
bnpl_earnings = pd.read_csv("../data/curated/BNPL_earnings.csv")
revenue = pd.read_csv("../data/curated/revenue.csv")
customers = pd.read_csv("../data/curated/customers.csv")
tags = pd.read_csv("../data/curated/tagged_merchants.csv")

# adjusting columns
customers = customers[['merchant_name', 'total_future_customers']]
revenue = revenue[['merchant_name', 'total_revenue']]
tags = tags[['name', 'merchant_abn', 'category']]
tags = tags.rename(columns={'name': 'merchant_name'})
transactions = transactions.rename(columns={'prediction': 'total_future_transactions'})


tags_trans = pd.merge(tags, transactions, on=['merchant_abn','merchant_name'],
how='inner')
add_customers = pd.merge(tags_trans, customers, on ='merchant_name', 
how='inner')
add_revenue = pd.merge(add_customers, revenue, on ='merchant_name', 
how='inner')
add_bnpl = pd.merge(add_revenue, bnpl_earnings, on ='merchant_name')
final = pd.merge(add_bnpl, fraud, on ='merchant_abn', how='inner')

#==============================================================================
# NORMALISE DATA
#==============================================================================
# copy the data

normalised = final.copy()

for feature in ['total_future_customers', 'total_revenue', 'total_earnings_of_BNPL',
               'total_future_transactions', 'average_fraud_rate_per_merchant']:
    normalised[feature] = (normalised[feature] - normalised[feature].min()) \
                        / (normalised[feature].max() - normalised[feature].min())    


#------------------------------------------------------------------------------
# adding in the weights
fraud_weights = int(weights[0])
transactions_weights = int(weights[1])
revenue_weights = int(weights[2])
customer_weights = int(weights[3])
bnpl_weights = int(weights[4])

normalised['ranking_feature'] = transactions_weights*normalised['total_future_transactions'] + \
                                revenue_weights*normalised['total_revenue'] + \
                                customer_weights*normalised['total_future_customers'] + \
                                bnpl_weights*normalised['total_earnings_of_BNPL'] + \
                                -1*fraud_weights*normalised['average_fraud_rate_per_merchant']

#------------------------------------------------------------------------------
# splitting by tags for top 10 merchants
tags = tags_trans.category.unique()

for tag in tags:
    print("Ranking for ", tag, "category: ")
    df = normalised.query("category == @tag")
    print("Number of merchants in this category: ",len(df))
    
    df = df.sort_values(by='ranking_feature', ascending=False)
    merchant_rank = df['merchant_name'].reset_index(drop = True)
    
    for i in range(10):
        print("Rank ", i+1, ": ", merchant_rank[i])
    print("\n")

#------------------------------------------------------------------------------
# top 100 merchants
final['ranking_feature'] = normalised['ranking_feature']
final_rank = final.sort_values(by='ranking_feature', ascending=False)
final_rank = final_rank.head(100)
final_rank = final_rank.reset_index(drop = True)

final_rank.to_csv("../data/curated/final_rank.csv", index = True)
