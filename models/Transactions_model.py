#==============================================================================
# Importing required libraries
import numpy as np
import pandas as pd
from pyspark.sql import SparkSession
from pyspark.sql.functions import year, month
from pyspark.sql.functions import when
from pyspark.sql.functions import max
from pyspark.sql.functions import lit
from sklearn.model_selection import train_test_split
from sklearn.linear_model import LinearRegression
from sklearn.metrics import r2_score
from sklearn.metrics import mean_squared_error
from sklearn.metrics import mean_absolute_error
#==============================================================================
# Create a spark session
#==============================================================================
spark = (
    SparkSession.builder.appName("MAST30034 Project 2")
    .config("spark.sql.repl.eagerEval.enabled", True) 
    .config("spark.sql.parquet.cacheMetadata", "true")
    .config("spark.sql.session.timeZone", "Etc/UTC")
    .config("spark.driver.memory", "15g")
    .config("spark.executor.memory", "5g")
    .getOrCreate()
)

#==============================================================================
# Read in data
#==============================================================================
data = spark.read.parquet("../data/tables/full_join.parquet")

#==============================================================================
# Aggregate data by merchant by month
#==============================================================================

data_month = data.withColumn('year',year(data.order_datetime))
data_month = data_month.withColumn('month',month(data_month.order_datetime))
#------------------------------------------------------------------------------
# Pivot data to create states and gender features
total_count_month = data_month.groupBy("merchant_name", "merchant_abn", 
"year", "month", "categories").count()

states_month = data_month.groupBy("merchant_name", "merchant_abn", "year", 
"month", "categories").pivot('state').count().fillna(0)

genders_month = data_month.groupBy("merchant_name", "merchant_abn", "year", 
"month", "categories").pivot('gender').count().fillna(0)

#------------------------------------------------------------------------------
# Rename columns
states_month = states_month.withColumnRenamed("merchant_name", 
    "merchant_name_st") \
    .withColumnRenamed("merchant_abn", 'merchant_abn_st') \
    .withColumnRenamed("year", 'year_st') \
    .withColumnRenamed("month", 'month_st') \
    .withColumnRenamed("categories", "categories_st")

genders_month = genders_month.withColumnRenamed("merchant_name", 
    "merchant_name_g") \
    .withColumnRenamed("merchant_abn", 'merchant_abn_g') \
    .withColumnRenamed("year", 'year_g') \
    .withColumnRenamed("month", 'month_g') \
    .withColumnRenamed("categories", "categories_g")

#------------------------------------------------------------------------------
# Join dataframes
joined_month = total_count_month.join(states_month,
        (total_count_month["merchant_name"] == states_month["merchant_name_st"
        ]) & \
        (total_count_month["merchant_abn"] == states_month["merchant_abn_st"]) 
        & \
        (total_count_month["year"] == states_month["year_st"]) & \
        (total_count_month["month"] == states_month["month_st"]))\
        .join(genders_month,(total_count_month["merchant_name"
        ] == genders_month["merchant_name_g"]) & \
        (total_count_month["merchant_abn"
        ] == genders_month["merchant_abn_g"]) & \
        (total_count_month["year"] == genders_month["year_g"]) & \
        (total_count_month["month"] == genders_month["month_g"]))

agg_month = joined_month.drop("merchant_name_st", "merchant_abn_st", "year_st",
                 "month_st", "categories_st", "merchant_name_g", 
                 "merchant_abn_g", "year_g", 
                              "month_g", "categories_g")

#==============================================================================
# Offset data by one month to create a feature containing transactions in the 
# future month
#==============================================================================
agg_projection = agg_month.select("merchant_name", "merchant_abn", "count", 
"year", "month")

agg_projection = agg_projection.withColumn("prev_year", \
            when(agg_projection["month"] == 1, agg_projection['year'
            ] - 1).otherwise(agg_projection['year']))
agg_projection = agg_projection.withColumn("prev_month", \
             when(agg_projection["month"] == 1, 12
            ).otherwise(agg_projection['month'] - 1))
agg_projection = agg_projection.drop("year", "month")
agg_projection = agg_projection.withColumnRenamed("count", "future_count") \
            .withColumnRenamed("merchant_name", "p_merchant_name") \
            .withColumnRenamed("merchant_abn", 
            "p_merchant_abn")

#------------------------------------------------------------------------------
# Join future data to past data
final_data = agg_month.join(agg_projection, 
        (agg_month.merchant_name == agg_projection.p_merchant_name) & 
        (agg_month.merchant_abn == agg_projection.p_merchant_abn) & 
        (agg_month.year == agg_projection.prev_year) & 
        (agg_month.month == agg_projection.prev_month), how = 'inner')

final_data = final_data.drop("p_merchant_name", "p_merchant_abn", 
"prev_year", "prev_month")
#==============================================================================
# Create linear model
#==============================================================================
# Prepare train and test split data
final_data_pd = final_data.toPandas()

y = final_data_pd['future_count']
x = final_data_pd.drop(['merchant_name', 'merchant_abn', 'categories', 
'future_count'], axis=1)

x_train, x_test, y_train, y_test = train_test_split(x, y, test_size = 0.2, 
random_state = 42)

#------------------------------------------------------------------------------
# Train linear model
LR = LinearRegression()
LR.fit(x_train,y_train)
y_prediction =  LR.predict(x_test)

#------------------------------------------------------------------------------
# Evaluating linear model
score=r2_score(y_test,y_prediction)
mae = mean_absolute_error(y_test, y_prediction)
rmse = np.sqrt(mean_squared_error(y_test,y_prediction))

#==============================================================================
# Use data from the latest month to predict future data
#==============================================================================
# Filter latest year
latest_year = agg_month.select(max('year')).collect()[0][0]
agg_month_1 = agg_month.filter(agg_month.year == latest_year)

# Filter latest month
latest_month = agg_month_1.select(max('month')).collect()[0][0]
predicting_data = agg_month_1.filter(agg_month.month == latest_month)

predicting_data = predicting_data.withColumn("future_count", lit(0))

#------------------------------------------------------------------------------
predicting_data_pd = predicting_data.toPandas()
x = predicting_data_pd.drop(['merchant_name', 'merchant_abn', 'categories', 
'future_count'], axis=1)

# Obtain predictions for future number of transactions
y_prediction =  LR.predict(x)
predicting_data_pd['prediction'] = y_prediction

# Save predictions
no_cust_ranking_feature = predicting_data_pd[['merchant_name','merchant_abn',
'prediction']]
no_cust_ranking_feature.to_csv("../data/curated/no_cust_ranking_feature.csv", 
index = False)
#------------------------------------------------------------------------------
