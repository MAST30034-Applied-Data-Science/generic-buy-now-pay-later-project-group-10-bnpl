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
#Reading in data: 

fraud_dataset = spark.read.parquet("../data/tables/avg_fraud_rate_per_merchant.parquet")
full_data = spark.read.parquet("../data/curated/full_join.parquet")

fraud_dataset = fraud_dataset.select("average_fraud_rate_per_merchant"
                                     ,"merchant_abn").withColumnRenamed("order_id","tags_order_id")
print(fraud_dataset.limit(5))

order_data = full_data.select("trans_merchant_abn","order_id")
tags_data = spark.read.parquet("../data/curated/tagged_merchants.parquet")
tags_data = tags_data.withColumnRenamed("merchant_abn","tags_merchant_abn").withColumnRenamed("order_id","tags_order_id")
# tags_data.limit(5)

#Joining data: 

fraud_dataset.createOrReplaceTempView("temp")

order_data.createOrReplaceTempView("temp2")

intermediate = spark.sql(""" 

SELECT *
FROM temp

INNER JOIN temp2

ON temp.merchant_abn = temp2.trans_merchant_abn
""")


intermediate.createOrReplaceTempView("temp")
tags_data.createOrReplaceTempView("temp2")
tags_vs_segment_data = spark.sql(""" 

SELECT *
FROM temp

INNER JOIN temp2

ON temp.merchant_abn = temp2.tags_merchant_abn
""")


#aggregating fraud rates by category: 

aggregated_fraud_by_category = tags_vs_segment_data.groupBy("category").agg(F.avg("average_fraud_rate_per_merchant").alias("fraud_rate_per_category"))

aggregated_fraud_by_category_pdf = aggregated_fraud_by_category.toPandas()
ax = aggregated_fraud_by_category_pdf.plot.bar(x='category', y='fraud_rate_per_category')
