#============================================================================================
# Import libraries
from pyspark.sql import SparkSession, functions as F
from pyspark.ml.feature import IndexToString, StringIndexer, VectorIndexer
from pyspark.ml.evaluation import MulticlassClassificationEvaluator
from pyspark.ml.feature import OneHotEncoder, VectorAssembler
from pyspark.sql.functions import col,isnan, when, count
from pyspark.ml.regression import RandomForestRegressor
from pyspark.ml.evaluation import RegressionEvaluator
from pyspark.sql.functions import date_format
from pyspark.sql.functions import year, month
import pandas as pd
import lbl2vec
import os 
import ETL

#==============================================================================
# Start a spark session
spark = (
    SparkSession.builder.appName("MAST30034 Project 2")
    .config("spark.sql.repl.eagerEval.enabled", True) 
    .config("spark.sql.parquet.cacheMetadata", "true")
    .config("spark.sql.session.timeZone", "Etc/UTC")
    .config("spark.driver.memory", "6g")
    .config("spark.executor.memory", "10g")
    .getOrCreate()
)
#==============================================================================
# STEP 1: Preprocess and prepare the main dataset
#==============================================================================
# ----------------------------------------------------------------------------
# Read the tagged merchant data and convert it to a parquet
tagged_merchants_sdf = spark.read.parquet("../data/curated/tagged_merchants.parquet")

# ----------------------------------------------------------------------------
# Change the column name for the merchant abn for later separation
tagged_merchants_sdf = tagged_merchants_sdf.withColumnRenamed('merchant_abn',

    'tagged_merchant_abn'
)

# ----------------------------------------------------------------------------
# Read the final dataset from the ETL script and join it to the tagged dataset

# Create a temporary view for the SQL query
ETL.final_join3.createOrReplaceTempView("join")
tagged_merchants_sdf.createOrReplaceTempView("tagged")

joint = spark.sql(""" 

SELECT *
FROM join
INNER JOIN tagged
ON join.merchant_abn = tagged.tagged_merchant_abn
""")

# Delete the redundant column
joint = joint.drop('tagged_merchant_abn')

# ----------------------------------------------------------------------------
# Calculate the share of the BNPL firm to be subtracted later from the dollar
# value to get the merchant revenue
joint.createOrReplaceTempView("group")

main_data = spark.sql(""" 

SELECT *, ((take_rate/100)*dollar_value) AS percent
FROM group

""")

# ----------------------------------------------------------------------------
# Extracting the year, month, day from the timestamp
main_data = main_data.withColumn('Year', year(main_data.order_datetime))
main_data = main_data.withColumn('Month',month(main_data.order_datetime))

# ----------------------------------------------------------------------------
main_data = main_data.drop('merchant_abn', 'categories','name', 'address', 
'trans_merchant_abn', 'order_id','order_datetime','user_id', 'consumer_id',
'int_sa2','SA2_name','state_code','state_name','population_2020',
'population_2021')

# ----------------------------------------------------------------------------
# Find Count of Null, None, NaN of All DataFrame Columns
main_data.select([count(when(isnan(c) | col(c).isNull(), c)).alias(c) for c in main_data.columns])

# ----------------------------------------------------------------------------
# Find the number of male and female customers for every merchant 
main_data.createOrReplaceTempView("agg")

male = spark.sql(""" 

SELECT CONCAT(merchant_name, SA2_code, Year, Month) AS m_name, COUNT(gender) as males
FROM agg
WHERE gender = 'Male'
GROUP BY merchant_name, SA2_code, Year, Month
""")


female = spark.sql(""" 

SELECT CONCAT(merchant_name, SA2_code, Year, Month) AS f_name, COUNT(gender) as females
FROM agg
WHERE gender = 'Female'
GROUP BY merchant_name, SA2_code, Year, Month
""")

# ----------------------------------------------------------------------------
# Aggregate the main data by merchant name, SA2 area code, year and month to
# later join with the count of male and female customers
main_data.createOrReplaceTempView("agg")

main_agg_data = spark.sql(""" 

SELECT merchant_name, COUNT(merchant_name) AS no_of_transactions, SA2_code, Year, Month, SUM(dollar_value - percent) AS total_earnings,
    CONCAT(merchant_name, SA2_code, Year, Month) AS join_col
FROM agg
GROUP BY merchant_name, SA2_code, Year, Month
""")

# ----------------------------------------------------------------------------
# Join the main aggregated data to the female and male customer counts
main_agg_data.createOrReplaceTempView("gender_join")
male.createOrReplaceTempView("male_agg")
female.createOrReplaceTempView("female_agg")

temp = spark.sql(""" 

SELECT *
FROM gender_join
INNER JOIN male_agg
ON gender_join.join_col = male_agg.m_name
""")

temp.createOrReplaceTempView("temp")

gender_agg = spark.sql(""" 

SELECT *
FROM temp
INNER JOIN female_agg
ON temp2.join_col = female_agg.f_name
""")

# ----------------------------------------------------------------------------
# Change the column name
main_data = main_data.withColumnRenamed('income_2018-2019',

    'income_2018_2019'    
)

# ----------------------------------------------------------------------------
# Calculate the income per person for each SA2 area code
main_data = main_data.withColumn('income_per_persons',
    (F.col('income_2018_2019')/F.col('total_persons'))
)

# ----------------------------------------------------------------------------
# Extract the values for revenue levels, category for every merchant and total
# females, males and income per perosn for each SA2 code which are constant for
# each mercahtn and SA2 code respectively 
main_data.createOrReplaceTempView("features")

other_agg = spark.sql(""" 

SELECT merchant_name AS drop_name, FIRST(take_rate) AS take_rate, FIRST(revenue_levels) AS revenue_levels, FIRST(category) AS category,
    FIRST(total_males) AS males_in_SA2, FIRST(total_females) AS females_in_SA2, FIRST(income_per_persons) AS income_per_person
FROM features
GROUP BY merchant_name
""")

# ----------------------------------------------------------------------------
# Join the above extracted values to the main dataset
gender_agg.createOrReplaceTempView("edit")
other_agg.createOrReplaceTempView("rates")

other_cols = spark.sql(""" 

SELECT *
FROM edit
INNER JOIN rates
ON edit.merchant_name = rates.drop_name
""")

# Drop the redundant columns
train = other_cols.drop('m_name', 'f_name', 'drop_name','join_col')

#==============================================================================
# STEP 2: Prepare a train and test dataset by offsetting the months by 1
#==============================================================================

# Select the main columns for offsetting
train_projection = train.select("merchant_name", "SA2_code", "Year", "Month", 'total_earnings')

# ----------------------------------------------------------------------------
# Offset the dataset by 1 month
train_projection = train_projection.withColumn("prev_year", \
              when(train_projection["Month"] == 1, train_projection['Year'] - 1).otherwise(train_projection['Year']))
train_projection = train_projection.withColumn("prev_month", \
              when(train_projection["Month"] == 1, 12).otherwise(train_projection['Month'] - 1))
train_projection = train_projection.drop("Year", "Month")
train_projection = train_projection.withColumnRenamed("total_earnings", "future_earnings") \
                            .withColumnRenamed("merchant_name", "p_merchant_name") \
                            .withColumnRenamed("SA2_code", "p_SA2_code")

# -----------------------------------------------------------------------------
# Join the offsetted values to the rest of the SA2 and aggregated values
final_data= train.join(train_projection, (train.merchant_name == train_projection.p_merchant_name) & 
                           (train.SA2_code == train_projection.p_SA2_code) & 
                           (train.Year == train_projection.prev_year) & 
                           (train.Month == train_projection.prev_month), how = 'inner')

# Drop the redundant columns
final_data = final_data.drop("p_merchant_name", "p_SA2_code","prev_year", "prev_month")

# -----------------------------------------------------------------------------
# Change the variable types
field_str = ['Year', 'Month', 'SA2_code']

for cols in field_str:
    final_data = final_data.withColumn(cols,

    F.col(cols).cast('STRING')

)

field_int = ['no_of_transactions', 'males', 'females', 'males_in_SA2', 'females_in_SA2']

for col in field_int:
    final_data = final_data.withColumn(col,

    F.col(col).cast('INT')

)

#==============================================================================
# STEP 3: Build and train the Random Forrest Model
#==============================================================================
# String indexing the categorical columns

indexer = StringIndexer(inputCols = ['merchant_name', 'SA2_code', 'Year', 'Month', 'revenue_levels','category'],
outputCols = ['merchant_name_num', 'SA2_code_num', 'Year_num', 'Month_num', 'revenue_levels_num','category_num'], handleInvalid="keep")

indexd_data = indexer.fit(final_data).transform(final_data)


# Applying onehot encoding to the categorical data that is string indexed above
encoder = OneHotEncoder(inputCols = ['merchant_name_num', 'SA2_code_num', 'Year_num', 'Month_num', 'revenue_levels_num','category_num'],
outputCols = ['merchant_name_vec', 'SA2_code_vec', 'Year_vec', 'Month_vec', 'revenue_levels_vec','category_vec'])

onehotdata = encoder.fit(indexd_data).transform(indexd_data)


# Assembling the training data as a vector of features 
assembler1 = VectorAssembler(
inputCols=['merchant_name_vec', 'SA2_code_vec', 'Year_vec', 'Month_vec', 'revenue_levels_vec','category_vec','males_in_SA2','females_in_SA2', 'income_per_person', 'no_of_transactions','take_rate', 'total_earnings'],
outputCol= "features" )

outdata1 = assembler1.transform(onehotdata)

# -----------------------------------------------------------------------------
# Renaming the target column as label

outdata1 = outdata1.withColumnRenamed(
    "future_earnings",
    "label"
)

# -----------------------------------------------------------------------------
# Assembling the features as a feature vector 

featureIndexer =\
    VectorIndexer(inputCol="features", 
    outputCol="indexedFeatures").fit(outdata1)

outdata1 = featureIndexer.transform(outdata1)

# -----------------------------------------------------------------------------
# Split the data into training and validation sets (30% held out for testing)

trainingData, testData = outdata1.randomSplit([0.7, 0.3], seed = 20)

# -----------------------------------------------------------------------------
# Train a RandomForest model.
rf = RandomForestRegressor(featuresCol="indexedFeatures")

# Train model.  
model = rf.fit(trainingData)

# Make predictions.
predictions_validation = model.transform(testData)

# -----------------------------------------------------------------------------
# Evaluate the validation set 

predictions_validation.select("prediction", "label", "features").show(5)

# Select (prediction, true label) and compute test error

evaluator_train_rmse = RegressionEvaluator(
    labelCol="label", predictionCol="prediction", metricName="rmse")
rmse_train = evaluator_train_rmse.evaluate(predictions_validation)


evaluator_train_mae = RegressionEvaluator(
    labelCol="label", predictionCol="prediction", metricName="mae")
mae_train = evaluator_train_mae.evaluate(predictions_validation)

# -----------------------------------------------------------------------------
# Define a funtion to extract the feature name of the most important features
def ExtractFeatureImportance(featureImp, dataset, featuresCol):
    list_extract = []
    for i in dataset.schema[featuresCol].metadata["ml_attr"]["attrs"]:
        list_extract = list_extract + dataset.schema[featuresCol].metadata["ml_attr"]["attrs"][i]
    varlist = pd.DataFrame(list_extract)
    varlist['score'] = varlist['idx'].apply(lambda x: featureImp[x])
    return(varlist.sort_values('score', ascending = False))
  
  # -----------------------------------------------------------------------------
# Extract the first five most important feature
dataset_fi = ExtractFeatureImportance(model.featureImportances, predictions_validation, "features")
dataset_fi = spark.createDataFrame(dataset_fi)

# -----------------------------------------------------------------------------
# Select the latest month from the latest year in the dataset which will be
# used as a test set for future predictions due to the offsetting done 
# previously
latest_year = train.select(max('Year')).collect()[0][0]
agg_month_1 = train.filter(train.Year == latest_year)
latest_month = agg_month_1.select(max('Month')).collect()[0][0]
predicting_data = agg_month_1.filter(train.Month == latest_month)
predicting_data = predicting_data.withColumn("future_earnings", lit(0))

#==============================================================================
# STEP 4: Make future predictions
#==============================================================================
# Repeat the indexing and vector assembling steps again for the test data

# String indexing the categorical columns

indexer = StringIndexer(inputCols = ['merchant_name', 'SA2_code', 'Year', 'Month', 'revenue_levels','category'],
outputCols = ['merchant_name_num', 'SA2_code_num', 'Year_num', 'Month_num', 'revenue_levels_num','category_num'], handleInvalid="keep")

indexd_data = indexer.fit(predicting_data).transform(predicting_data)


# Applying onehot encoding to the categorical data that is string indexed above
encoder = OneHotEncoder(inputCols = ['merchant_name_num', 'SA2_code_num', 'Year_num', 'Month_num', 'revenue_levels_num','category_num'],
outputCols = ['merchant_name_vec', 'SA2_code_vec', 'Year_vec', 'Month_vec', 'revenue_levels_vec','category_vec'])

onehotdata = encoder.fit(indexd_data).transform(indexd_data)


# Assembling the training data as a vector of features 
assembler1 = VectorAssembler(
inputCols=['merchant_name_vec', 'SA2_code_vec', 'Year_vec', 'Month_vec', 'revenue_levels_vec','category_vec','males_in_SA2','females_in_SA2', 'income_per_person', 'no_of_transactions','take_rate', 'total_earnings'],
outputCol= "features" )

outdata1 = assembler1.transform(onehotdata)

# Renaming the target column as label

outdata1 = outdata1.withColumnRenamed(
    "future_earnings",
    "label"
)

# Assembling the features as a feature vector 

featureIndexer =\
    VectorIndexer(inputCol="features", 
    outputCol="indexedFeatures").fit(outdata1)

outdata1 = featureIndexer.transform(outdata1)

# -----------------------------------------------------------------------------
# Fit the model to the test dataset
predictions_test = model.transform(outdata1)

# -----------------------------------------------------------------------------
# Aggregate the predicted data to merchant level to get the total predicted 
# merchant revenue
predictions_test.createOrReplaceTempView("preds")

pred = spark.sql(""" 

SELECT merchant_name, SUM(prediction) AS total_revenue
FROM preds
GROUP BY merchant_name

""")

# -----------------------------------------------------------------------------
# Convert the predicted data to pandas and save as a csv
pred_df = pred.toPandas()
pred_df.to_csv("../data/curated/revenue.csv")

# -----------------------------------------------------------------------------