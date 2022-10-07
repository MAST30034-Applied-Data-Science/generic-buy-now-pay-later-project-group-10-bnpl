#============================================================================================
import pandas as pd
from pyspark.sql.functions import col
from pyspark.sql import functions as F
from pyspark.sql.functions import when
from pyspark.sql import SparkSession
from pyspark.sql import functions as countDistinct

#============================================================================================
# Create a spark session
#============================================================================================
spark = (
    SparkSession.builder.appName("MAST30034 Project 2")
    .config("spark.sql.repl.eagerEval.enabled", True)
    .config("spark.sql.parquet.cacheMetadata", "true")
    .config("spark.sql.session.timeZone", "Etc/UTC")
    .config("spark.driver.memory", "15g")
    .config("spark.executor.memory", "5g")
    .getOrCreate()
)

#============================================================================================
# LOAD THE DATA
#============================================================================================
# Retrive the required data

#Loading consumer fraud data:
fraud_consumer = pd.read_csv('../data/tables/consumer_fraud_probability.csv')
fraud_consumer = spark.createDataFrame(fraud_consumer)

#Loading merchant fraud data:
fraud_merchant = pd.read_csv('../data/tables/merchant_fraud_probability.csv')
fraud_merchant = spark.createDataFrame(fraud_merchant)

full_data = spark.read.parquet('../data/tables/full_join.parquet')

segments_data = spark.read.csv('../data/curated/tagged_merchants.csv')

#============================================================================================

#============================================================================================
# JOINING THE DATA TABLES
#============================================================================================

#Renaming columns to avoid duplicates
fraud_consumer = fraud_consumer.withColumnRenamed("fraud_probability","cons_fraud_prob")\
.withColumnRenamed("order_datetime","order_datetime_1").withColumnRenamed("user_id","user_id_1")

fraud_merchant = fraud_merchant.withColumnRenamed("merchant_abn","merchant_abn_1").withColumnRenamed\
("fraud_probability","merch_fraud_prob")

#Joining consumer fraud and merchant fraud data together

merchant_data = full_data.select\
("merchant_abn", "categories","revenue_levels","dollar_value","user_id","consumer_id")
merchant_data.createOrReplaceTempView("temp")

fraud_consumer.createOrReplaceTempView("temp2")

join_1 = spark.sql("""

SELECT *
FROM temp

LEFT JOIN temp2

ON temp.user_id = temp2.user_id_1
""")

#Joining the intermediate dataset with consumer fraud data
join_1.createOrReplaceTempView("temp")

fraud_merchant.createOrReplaceTempView("temp2")

final_join = spark.sql("""

SELECT *
FROM temp


LEFT JOIN temp2

ON temp.merchant_abn = temp2.merchant_abn_1
""")

#Joining the intermediate dataset with segments

#Removing false header line
segments_data.na.drop(subset=["_c0"])

final_join.createOrReplaceTempView("temp")

segments_data.createOrReplaceTempView("temp2")

final_join = spark.sql("""

SELECT *
FROM temp


LEFT JOIN temp2

ON temp.merchant_abn = temp2._c3
""")

#============================================================================================
# EVALUATING AVERAGE FRAUD PROBABILITIES
#============================================================================================

#Creating a table of average fraud probabilities for each category of purchase:

aggregated_prob = final_join.groupBy("temp2._c6").agg(F.avg("cons_fraud_prob").\
alias("avg_consumer_fraud"),F.avg("merch_fraud_prob").alias("avg_merchant_fraud"))

#adding a column for is_fraud BOOLEAN value for each transaction:

is_fraud = when((final_join["_c6"] == "Electronics") & (final_join["cons_fraud_prob"] > 15.085)&(final_join["merch_fraud_prob"]>29.635),1)\
.when((final_join["_c6"] == "Toys and DIY") & (final_join["cons_fraud_prob"] > 15.843) &(final_join["merch_fraud_prob"]>32.404),1)\
.when((final_join["_c6"] == "Furniture") & (final_join["cons_fraud_prob"] > 15.077) & (final_join["merch_fraud_prob"]>30.941),1)\
.when((final_join["_c6"] == "Beauty, Health, Personal and Household") & (final_join["cons_fraud_prob"] > 15.392) & (final_join["merch_fraud_prob"]>29.76),1)\
.when((final_join["_c6"] == "Electronics") & (final_join["cons_fraud_prob"] > 15.085) &(final_join["merch_fraud_prob"]>29.635),1)\
.when((final_join["_c6"] == "Books, Stationary and Music") & (final_join["cons_fraud_prob"] > 15.140) &(final_join["merch_fraud_prob"]>29.016),1).otherwise(0)

# joining the boolean column to the rest of the data
model_with_fraud = final_join.withColumn("is_fraud",is_fraud)

#joining model_with_fraud with postcodes data for visualisations

#cat = spark.read.parquet('/Users/ahirve/Desktop/ads_bnpl/data/tables/transactions_with_fraud_rates.parquet')
#cat.limit(5)

model_with_fraud = model_with_fraud.withColumnRenamed("merchant_abn","sus_merchant_abn")
full_data = spark.read.parquet('../data/tables/full_join.parquet')

full_data.createOrReplaceTempView("temp")

model_with_fraud.createOrReplaceTempView("temp2")

full_data_with_fraud = spark.sql("""

SELECT *
FROM temp


LEFT JOIN temp2

ON temp.merchant_abn = temp2.sus_merchant_abn
""")





#============================================================================================
# SAVING FINAL DATASETS
#============================================================================================

model_with_fraud.write.mode("overwrite").parquet("../data/tables/transactions_with_fraud_rates.parquet")
full_data_with_fraud.write.parquet('../data/tables/postcodes_with_fraud_rates.parquet')

