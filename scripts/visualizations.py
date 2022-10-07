
#==============================================================================
import matplotlib.pyplot as plt
import outlier
import pandas as pd
import seaborn as sns
import folium
from pyspark.sql.functions import date_format
from pyspark.sql import SparkSession, functions as F

#==============================================================================
# Create a spark session
spark = (
    SparkSession.builder.appName("MAST30034 Project 2")
    .config("spark.sql.repl.eagerEval.enabled", True) 
    .config("spark.sql.parquet.cacheMetadata", "true")
    .config("spark.sql.session.timeZone", "Etc/UTC")
    .config("spark.driver.memory", "10g")
    .getOrCreate()
)

#------------------------------------------------------------------------------
# Plot the total male and female transactions
#print("The distribution of transactions according to gender")
fig1, ax1 = plt.subplots()
genders = outlier.internal4.select("gender")
genderspd = genders.toPandas()
sns.countplot(data=genderspd, x="gender")
ax1.ticklabel_format(style='plain', axis='y')
ax1.set_xlabel("Gender")


#------------------------------------------------------------------------------
# Distribution of total revenue for each merchant from online purchases
#print("The distribution of total revenue across all the merchants")
selected_columns = outlier.internal4.select("merchant_abn","dollar_value")
aggregated_revenue = selected_columns.groupby("merchant_abn").sum("dollar_value")
aggregated_revenue_pd = aggregated_revenue.toPandas()
total_revenue = aggregated_revenue_pd['sum(dollar_value)']

fig2, ax2 = plt.subplots()
sns.boxplot(total_revenue)
ax2.set_xlabel("Total revenue per merchant")

#------------------------------------------------------------------------------
# Distributions of transactions by state
#print("Number of transactions per Australian state")
state = outlier.internal4.select("state")
statepd = state.toPandas()

fig3, ax3 = plt.subplots()
sns.countplot(data=statepd, x="state")
ax3.ticklabel_format(style='plain', axis='y')
ax3.set_xlabel("Gender")


#------------------------------------------------------------------------------
# Number of transactions made per month

# Read the tagged model
tagged_merchants_sdf = spark.read.parquet("../data/curated/tagged_merchants.parquet")

# -----------------------------------------------------------------------------
# Join the final dataset to the tagged model
tagged_merchants_sdf = tagged_merchants_sdf.withColumnRenamed('merchant_abn',

    'tagged_merchant_abn'
)
# -----------------------------------------------------------------------------
# Calculate the BNPL earnings 
outlier.internal4.createOrReplaceTempView("join")
tagged_merchants_sdf.createOrReplaceTempView("tagged")

joint = spark.sql(""" 

SELECT *
FROM join
INNER JOIN tagged
ON join.merchant_abn = tagged.tagged_merchant_abn
""")

# -----------------------------------------------------------------------------
# Calculate the BNPL earnings 
joint = joint.drop('tagged_merchant_abn')


transactions = joint.withColumn("Year", 
date_format('order_datetime', 'yyyy'))

transactions = transactions.withColumn("Month", 
date_format('order_datetime', 'MMMM'))


transactions.createOrReplaceTempView("temp_view")

trans_2021 = spark.sql(""" 

SELECT Month, category, COUNT(merchant_abn) as transactions_2021
FROM temp_view
WHERE Year == '2021'
GROUP BY Month, category

""")

trans_2022 = spark.sql(""" 

SELECT Month, category, COUNT(merchant_abn) as transactions_2022
FROM temp_view
WHERE Year == '2022'
GROUP BY Month, category

""")


trans_2021_df = trans_2021.toPandas()
trans_2022_df = trans_2022.toPandas()


month_2021 = ['February', 'March', 'April', 'May', 'June', 'July', 'August', 
'September', 'October', 'November','December']
month_2022 = ['Janaury', 'February', 'March', 'April', 'May', 'June', 'July', 
'August', 'September']



# Order the month column 


trans_2021_df['Month'] = pd.Categorical(trans_2021_df['Month'], 
categories = month_2021, ordered=True)
trans_2021_df.sort_values(by = "Month", inplace = True)

trans_2022_df['Month'] = pd.Categorical(trans_2022_df['Month'], 
categories = month_2022, ordered=True)
trans_2022_df.sort_values(by = "Month", inplace = True)



fig4, ax4 = plt.subplots(figsize=(12,7))
sns.lineplot(data=trans_2021_df, x="Month", y="transactions_2021", 
hue="category")
ax4.set_ylabel("Number of transactions in 2021")



fig5, ax5 = plt.subplots(figsize=(12,7))
sns.lineplot(data=trans_2022_df, x="Month", y="transactions_2022", 
hue="category")
ax5.set_ylabel("Number of transactions in 2022")

#------------------------------------------------------------------------------
# Geospatial visualizations of number of transactions by post code
num_transactions_by_postcode = outlier.internal4.groupBy('postcodes')
postcodes_data1 = spark.read.option("header", 
True).csv('../data/visualisations_postcodes.csv')
postcodes_data2 = postcodes_data1.withColumnRenamed('Postcode', 'postcodes')
postcodes_data = postcodes_data2.dropDuplicates(['postcodes'])

transactions_location = num_transactions_by_postcode.join(postcodes_data,
                        ['postcodes'], 'inner')


# conver transactions location to pandas df
transactions_location_pdf = transactions_location.toPandas()

aus_coords = [-25.2744, 133.7751]
m = folium.Map(aus_coords, tiles='OpenStreetMap', zoom_start=4.5)

for index, row in transactions_location_pdf.iterrows():
    if row['count'] >= 10000:
        marker_color = 'darkred'
        fill_color = 'darkred'
    elif row['count'] >= 5000:
        marker_color = 'red'
        fill_color = 'red'
    elif row['count'] >= 500:
        marker_color = 'darkorange'
        fill_color = 'darkorange'
    elif row['count'] >= 100:
        marker_color = 'orange'
        fill_color = 'orange'
    elif row['count'] <= 50 :
        marker_color = 'yellow'
        fill_color = 'yellow'
    else:
        marker_color='darkpurple'
        fill_color = 'darkpurple'
        
    folium.Circle(
          location=[row['Lat'], row['Lon']],
          popup= 'Number of transactions: ' +str(row['count']),
          tooltip=row['Suburb'],
          radius=row['count'],
          color=marker_color,
          fill=True,
          fill_color=fill_color,
       ).add_to(m)
m.save('../plots/bubble_plot_num_transactions_by_location.html')

# Display the map
m 