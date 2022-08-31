#============================================================================================
import pandas as pd
from pyspark.sql import SparkSession
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

#--------------------------------------------------------------------------------------------