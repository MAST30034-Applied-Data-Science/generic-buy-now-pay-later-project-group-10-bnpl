#============================================================================================
import pandas as pd
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




#============================================================================================
# SCHEMAS
#============================================================================================