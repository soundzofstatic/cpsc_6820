import sys
from pyspark import SparkContext, SparkConf, SQLContext

sc = SparkContext("local", "First App")

sqlCtx = SQLContext(sc)
amzDF = sqlCtx.jsonFile("project/amazon-meta.small.json")
amzDF.registerTempTable("purchases")

## Search by X review count
searchReviewCount = sqlCtx.sql(""" SELECT * FROM purchases WHERE reviews['avg rating'] >= 4 """)
for i in searchReviewCount.collect(): print ("Matched Purchase: " + i.Id + ", Rating avg: " + i.reviews['avg rating'] + ", title: " + i.title)