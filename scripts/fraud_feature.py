#============================================================================================
import pandas as pd
from pyspark.sql.functions import col
from pyspark.sql import functions as F
from pyspark.sql.functions import when
from pyspark.sql import SparkSession
from pyspark.sql import functions as countDistinct
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
#Joining consumer fraud and merchant fraud data together

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

#============================================================================================
# SAVING FINAL DATASET
#============================================================================================

model_with_fraud.write.parquet("../data/tables/transactions_with_fraud_rates.parquet")
