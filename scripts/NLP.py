import pandas as pd
from pyspark.sql import SparkSession, functions as F
import lbl2vec
from sklearn.feature_extraction.text import TfidfVectorizer
from sklearn.cluster import KMeans
import numpy as np


# Create a spark session
spark = (
    SparkSession.builder.appName("MAST30034 Project 2")
    .config("spark.sql.repl.eagerEval.enabled", True) 
    .config("spark.sql.parquet.cacheMetadata", "true")
    .config("spark.sql.session.timeZone", "Etc/UTC")
    .config("spark.driver.memory", "10g")
    .getOrCreate()
)

consumer = pd.read_csv("/Users/Kasturi/Documents/GitHub/generic-buy-now-pay-later-project-group-10-bnpl/data/tables/tbl_consumer.csv", delimiter="|")

merchants = spark.read.parquet("/Users/Kasturi/Documents/GitHub/generic-buy-now-pay-later-project-group-10-bnpl/data/tables/tbl_merchants.parquet")

merchants_df = merchants.toPandas()

# Separate tags from revenue and take rate
tags = merchants_df["tags"].str.split("\), ", expand=True)
tags = tags[0].str.split("\], ", expand=True)
# Remove symbols from tag and making everything lowercase
tags = tags[0].str.replace('[^\w\s]', '', regex = True)
tags = tags.str.lower()


merchants_df['cleaned_tags'] = tags

from sklearn.feature_extraction.text import CountVectorizer
cv = CountVectorizer(max_df=0.95, min_df=2, stop_words='english')
dtm = cv.fit_transform(merchants_df['cleaned_tags'])

categories_label = ["fashion", "furniture", "electronics", "beauty, health, personal and household", "toys, hobbies and DIY"]


from sklearn.decomposition import LatentDirichletAllocation
LDA = LatentDirichletAllocation(n_components=5,random_state=42)
LDA.fit(dtm)
for index,topic in enumerate(LDA.components_):
    print(f'THE TOP 15 WORDS FOR TOPIC #{index}')
    print([cv.get_feature_names()[i] for i in topic.argsort()[-15:]])
    print('\n')


topic_results = LDA.transform(dtm)
merchants_df['store_type'] = topic_results.argmax(axis=1)


myDict = {0 : 'Furniture' , 1 : 'Toys and DIY', 2 : 'Beauty, Health, Personal and Household', 3 : 'Books, Stationary and Music', 4 : 'Electronics' }
 
merchants_df['category'] = merchants_df['store_type'].map(myDict)


