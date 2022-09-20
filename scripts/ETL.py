#============================================================================================
import pandas as pd
from pyspark.sql import SparkSession
from pyspark.sql import SparkSession
from pyspark.sql.types import LongType
from pyspark.sql.types import IntegerType
from pyspark.sql import functions as F
from pyspark.sql.functions import *
import json
#--------------------------------------------------------------------------------------------
# Create a spark session
spark = (
    SparkSession.builder.appName("MAST30034 Project 2")
    .config("spark.sql.repl.eagerEval.enabled", True) 
    .config("spark.sql.parquet.cacheMetadata", "true")
    .config("spark.sql.session.timeZone", "Etc/UTC")
    .config("spark.driver.memory", "10g")
    .getOrCreate()
)

#============================================================================================
# LOAD IN DATA FROM TABLES DIRECTORY
#============================================================================================
# Define relative target directory
with open("../scripts/paths.json") as json_paths: 
    PATHS = json.load(json_paths)
    json_paths.close()

raw_path = PATHS['raw_path']
curated_path = PATHS['curated_path']
#--------------------------------------------------------------------------------------------
    
# TBL Consumer
tbl_consumer = spark.read.option("header", True).csv(raw_path +'tbl_consumer.csv', sep='|')

#--------------------------------------------------------------------------------------------
# TBL Merchants
tbl_merchants = spark.read.parquet(raw_path + 'tbl_merchants.parquet')

#--------------------------------------------------------------------------------------------
# Consumer User Details
user_details = spark.read.parquet(raw_path + 'consumer_user_details.parquet')

#--------------------------------------------------------------------------------------------
# Transactions
transactions1 = spark.read.parquet(raw_path + 'transactions_20210228_20210827_snapshot/')
transactions2 = spark.read.parquet(raw_path + 'transactions_20210828_20220227_snapshot/')
transactions3 = spark.read.parquet(raw_path + 'transactions_20220228_20220828_snapshot/')
transactions12 = transactions1.union(transactions2).distinct()
transactions = transactions12.union(transactions3).distinct()

#--------------------------------------------------------------------------------------------
# Fraud Details
fraud_consumer = spark.read.option("header", True).csv(raw_path +'consumer_fraud_probability.csv')
fraud_merchants = spark.read.option("header", True).csv(raw_path +'merchant_fraud_probability.csv')

#============================================================================================
# Extract time periods (years) from transactions dataset

transactions = transactions.orderBy("order_datetime")

first_transaction_date = transactions.select(first("order_datetime").alias('date'))
first_transaction_year = first_transaction_date.withColumn("year", year(col('date')))

last_transaction_date = transactions.select(last("order_datetime").alias('date'))
last_transaction_year = last_transaction_date.withColumn("year", year(col('date')))

start_year = first_transaction_year.head()[1]
end_year = last_transaction_year.head()[1]

useful_years = list(range(start_year, end_year+1))


#============================================================================================
# PREPROCESSING MERCHANTS DATA
# Remove outer brackets in tags
df = tbl_merchants.withColumn("tags", F.regexp_replace("tags", "[\])][\])]", "")) \
        .withColumn("tags", F.regexp_replace("tags", "[\[(][\[(]", "")) 

# separate tags into categories, take rate, and revenue level
# convert take rate to double
tbl_merchants = df.withColumn('categories', F.split(df['tags'], '[)\]], [\[(]').getItem(0)) \
        .withColumn('take_rate', F.split(df['tags'], '[)\]], [\[(]take rate: ').getItem(1).cast("double")) \
        .withColumn('revenue_levels', F.split(df['tags'], '[)\]], [\[(]').getItem(1)) \
        .drop(F.col("tags")) \
        .withColumnRenamed("name", "merchant_name")
#--------------------------------------------------------------------------------------------
# PREPROCESSING CONSUMER DATA
# Change consumer_id from string to long type
tbl_consumer = tbl_consumer.withColumn("int_consumer_id", tbl_consumer["consumer_id"].cast(LongType())) \
        .drop(F.col("consumer_id"))

#--------------------------------------------------------------------------------------------
# PREPROCESSING TRANSACTIONS DATA
transactions = transactions.withColumnRenamed("merchant_abn", "trans_merchant_abn") \
        .withColumnRenamed("user_id", "trans_user_id")

#============================================================================================
# PERFORMING JOINS

# Join transactions to user details
trans_user = transactions.join(user_details,transactions.trans_user_id ==  user_details.user_id,"inner")

# Join consumer to above data
add_consumer = tbl_consumer.join(trans_user, tbl_consumer.int_consumer_id ==  trans_user.consumer_id,"inner")

# Join merchants to above data
final_join = tbl_merchants.join(add_consumer, tbl_merchants.merchant_abn == add_consumer.trans_merchant_abn, "full_outer") \
        .drop(F.col("int_consumer_id")) \
        .drop(F.col("trans_user_id"))

#============================================================================================
# Adding postcode stuff in 
#============================================================================================
postcode = spark.read.option("header", True).csv(curated_path +'postcode.csv')
postcode = postcode.drop(F.col("_c0"))
postcode_nonull = postcode.na.drop()
distinct = postcode_nonull.dropDuplicates(["postcodes"])

final_join2 = final_join.join(distinct, final_join.postcode == distinct.postcodes, "inner") \
        .drop(F.col("postcodes"))

final_join2 = final_join2.withColumn("int_SA2", final_join2["SA2"].cast(IntegerType())).drop(F.col("SA2"))

#============================================================================================
final_join2.write.mode('overwrite').parquet("../data/tables/full_join.parquet")

