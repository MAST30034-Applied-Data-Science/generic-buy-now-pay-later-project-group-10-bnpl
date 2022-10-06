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
### REMOVE OUTLIERS as detected in outlier analysis notebook
#--------------------------------------------------------------------------------------------
final_dataset = ETL.final_join3.filter("merchant_abn IS NOT NULL")

#--------------------------------------------------------------------------------------------
