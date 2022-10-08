import pandas as pd

BNPL_metrics = pd.read_csv("../data/curated/BNPL_metrics.csv")
BNPL_features = pd.read_csv("../data/curated/BNPL_features.csv")

customer_metrics = pd.read_csv("../data/curated/customer_metrics.csv")
customer_features = pd.read_csv("../data/curated/customer_features.csv")

revenue_metrics = pd.read_csv("../data/curated/revenue_metrics.csv")
revenue_features = pd.read_csv("../data/curated/revenue_features.csv")

transactions_metrics = pd.read_csv("../data/curated/transactions_metrics.csv")
transactions_features = pd.read_csv("../data/curated/transactions_features.csv")

metrics = {
    'Model name': ['Predicted BNPL earnings', 'Predicted no. of Customers', 
                        'Predicted Merchant Revenue', 
                        'Predicted no. of Transactions'],
    'Mean Absolute Error': [BNPL_metrics['MAE'].values[0], 
                        customer_metrics['MAE'].values[0],
                        revenue_metrics['MAE'].values[0],
                        transactions_metrics['MAE'].values[0]],
    'RMSE': [BNPL_metrics['RMSE'].values[0], customer_metrics['RMSE'].values[0],
                        revenue_metrics['RMSE'].values[0],
                        transactions_metrics['RMSE'].values[0]]
}

metrics_df = pd.DataFrame(metrics)


BNPL_features['name'][0]
feature = {
    'Model name': ['Predicted BNPL earnings', 'Predicted no. of Customers', 
                        'Predicted Merchant Revenue',
                        'Predicted no. of Transactions'],
    'First Feature': [BNPL_features['name'][0], customer_features['name'][0],
                revenue_features['name'][0], transactions_features['name'][0]],
    'Second Feature': [BNPL_features['name'][1],customer_features['name'][1],
                revenue_features['name'][1],transactions_features['name'][1]],
    'Third Feature': [BNPL_features['name'][2], customer_features['name'][2],
                revenue_features['name'][2], transactions_features['name'][2]]
}

feature_df = pd.DataFrame(feature)

