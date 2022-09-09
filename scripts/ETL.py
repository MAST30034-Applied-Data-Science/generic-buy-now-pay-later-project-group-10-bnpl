#============================================================================================
import pandas as pd
from pyspark.sql import SparkSession
from pyspark.sql import SparkSession
from pyspark.sql.types import LongType
from pyspark.sql import functions as F
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
# TBL Consumer
tbl_consumer = spark.read.option("header", True).csv("../data/tables/tbl_consumer.csv", sep='|')

#--------------------------------------------------------------------------------------------
# TBL Merchants
tbl_merchants = spark.read.parquet("../data/tables/tbl_merchants.parquet")

#--------------------------------------------------------------------------------------------
# Consumer User Details
user_details = spark.read.parquet("../data/tables/consumer_user_details.parquet")

#--------------------------------------------------------------------------------------------
# Transactions
transactions = spark.read.parquet('../data/tables/transactions_20210228_20210827_snapshot/')

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

#--------------------------------------------------------------------------------------------
final_join.write.mode('overwrite').parquet("../data/tables/full_join.parquet")