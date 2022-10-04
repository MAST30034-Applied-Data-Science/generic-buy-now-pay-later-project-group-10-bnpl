#============================================================================================
from pyspark.sql import SparkSession
from pyspark.sql import SparkSession
import pandas as pd
import ETL

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
### INTERNAL DATASET
#--------------------------------------------------------------------------------------------
internal = ETL.final_join
# print(internal.head(1))
# internal.printSchema()

print("Count before outliers: ", internal.count())
# Excluding transactions with no merchants
internal1 = internal.filter("merchant_abn IS NOT NULL")
print("Count after outlier exclusion 1: ", internal1.count())

# # Excluding transactions with $0
# internal2 = internal1.filter(internal1.dollar_value > 0)
# print("Count after outlier exclusion 2: ", internal2.count())

# # Excluding merchants with no transactions and record merchant name
# merchants_no_trans = internal2.filter("consumer_id IS NULL")
# internal3 = internal2.filter("consumer_id IS NOT NULL")
# print("Count after outlier exclusion 3: ", internal3.count())

#--------------------------------------------------------------------------------------------
selected_columns = internal1.select("merchant_abn","dollar_value")
aggregated_revenue = selected_columns.groupby("merchant_abn").sum("dollar_value")
aggregated_revenue_pd = aggregated_revenue.toPandas()

#--------------------------------------------------------------------------------------------










#--------------------------------------------------------------------------------------------