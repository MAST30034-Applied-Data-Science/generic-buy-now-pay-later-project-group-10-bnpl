#============================================================================================
from pyspark.sql import SparkSession
import read_data
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
# COUNTS
#============================================================================================

print("Counts for transactions: ", read_data.transactions.count())
print("Counts for user_details: ", read_data.user_details.count())
print("Counts for tbl_merchants: ", read_data.tbl_merchants.count())
print("Counts for tbl_consumer: ", read_data.tbl_consumer.count())

#============================================================================================
# SCHEMAS
#============================================================================================

print("Schema for transactions")
read_data.transactions.printSchema()

print("Schema for user_details")
read_data.user_details.printSchema()

print("Schema for tbl_merchants")
read_data.tbl_merchants.printSchema()

print("Schema for tbl_consumer")
read_data.tbl_consumer.printSchema()

#--------------------------------------------------------------------------------------------



