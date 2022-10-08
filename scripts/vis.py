#Covid Cases vs Number of Transactions per month

transactions = spark.read.parquet("../data/tables/full_join.parquet")

covid_transaction_data = transactions.select("merchant_abn", "order_id", "order_datetime")

covid_transaction_data = covid_transaction_data.select(col("order_datetime"),\
to_date(col("order_datetime"),"MM-yyyy").alias("temp_date"),col("order_id"))

covid_transaction_data = covid_transaction_data.withColumn\
("month", month(col("temp_date")))
covid_transaction_data = covid_transaction_data.withColumn\
("year", year(col("temp_date")))

transactions_2021 = covid_transaction_data.filter(covid_transaction_data.year == 2021)
transactions_2022 = covid_transaction_data.filter(covid_transaction_data.year == 2021)

transactions_month_2021 = transactions_2021.groupBy("month").agg(F.count("order_id").\
alias("num_transactions_month"))

transactions_month_2022 = transactions_2022.groupBy("month").agg(F.count("order_id").\
alias("num_transactions_month"))

covid_case_data = pd.read_csv("../data/curated/covid.csv")
covid_case_sdf = spark.createDataFrame(covid_case_data)

#Inner join:

covid_transaction_data.createOrReplaceTempView("temp")

covid_case_sdf.createOrReplaceTempView("temp2")

covid_plot_data = spark.sql("""

SELECT *
FROM temp


INNER JOIN temp2

ON temp.temp_date = temp2.date
""")

#Filtering
data_2021 = covid_plot_data.filter(covid_plot_data.year == 2021)
data_2022 = covid_plot_data.filter(covid_plot_data.year == 2022)

aggregated_data_2021 = data_2021.groupBy("mm").agg(F.sum("covid_cases").\
alias("total_covid_cases_month"))

aggregated_data_2022 = data_2022.groupBy("mm").agg(F.sum("covid_cases").\
alias("total_covid_cases_month"))

#Joins

#2021

transactions_month_2021.createOrReplaceTempView("temp")

aggregated_data_2021.createOrReplaceTempView("temp2")

covid_plot_data_2021 = spark.sql("""

SELECT *
FROM temp


INNER JOIN temp2

ON temp.month = temp2.mm
""")

#2022

transactions_month_2021.createOrReplaceTempView("temp")

aggregated_data_2022.createOrReplaceTempView("temp2")

covid_plot_data_2022 = spark.sql("""

SELECT *
FROM temp


INNER JOIN temp2

ON temp.month = temp2.mm
""")

covid_plot_data_2021_pdf = covid_plot_data_2021.toPandas()
covid_plot_data_2022_pdf = covid_plot_data_2022.toPandas()

covid_plot_data_2021_pdf.plot(x='total_covid_cases_month', y='num_transactions_month', style='o')
covid_plot_data_2022_pdf.plot(x='total_covid_cases_month', y='num_transactions_month', style='o')
