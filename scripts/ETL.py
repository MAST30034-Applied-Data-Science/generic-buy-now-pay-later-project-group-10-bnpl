#============================================================================================
from pyspark.sql import SparkSession
from pyspark.sql.types import LongType
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
#--------------------------------------------------------------------------------------------
transactions = read_data.transactions
tbl_consumer = read_data.tbl_consumer
user_details = read_data.user_details

#--------------------------------------------------------------------------------------------

hello = transactions.join(user_details,transactions.user_id ==  user_details.user_id,"inner")
print(hello.count())

hello2 = tbl_consumer.withColumn("int_consumer_id", tbl_consumer["consumer_id"].cast(LongType()))
print(hello2.limit(5))

hello3 = hello2.join(user_details,hello2.int_consumer_id ==  user_details.consumer_id,"inner")
print(hello3.count())
print(hello3.limit(5))