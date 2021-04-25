import sys
from pyspark import SparkContext, SparkConf, SQLContext
from pyspark.sql.functions import col, explode

sc = SparkContext("local", "First App")

sqlCtx = SQLContext(sc)
amzDF = sqlCtx.read.json("amazon-meta.small.json")
amzDF.registerTempTable("purchases")
print('---- Schema')
amzDF.printSchema()
print('-/-- Schema')

# Arguments
## 1 - search program

try:

    if len(sys.argv) <= 1:
        raise Exception ('Argument 1, utility name, must be specified')

    if sys.argv[1] == 'search-customer-rating':

        # Arguments
        ## 2 - operator, eg. >, >=, =, <, <=
        ## 3 - avg rating

        if len(sys.argv) < 3:
            # todo - should throw an error
            print('ERRORR!!!!!!')

        # searchCustomerId = sqlCtx.sql(" SELECT * FROM purchases WHERE reviews.rating > {} ".format(sys.argv[2]))
        # for i in searchCustomerId.collect(): print(
        #     "Matched Product: " + i.Id + ", Customer: " + i.reviews.rating + ", title: " + i.title)
        amzDF.withColumn("reviews", explode("reviews")).select("*", col("reviews.*")).show()

except Exception as err:
    print(err)

# ---- Schema
# root
#  |-- ASIN: string (nullable = true)
#  |-- Id: string (nullable = true)
#  |-- categories: array (nullable = true)
#  |    |-- element: string (containsNull = true)
#  |-- group: string (nullable = true)
#  |-- reviews: array (nullable = true)
#  |    |-- element: struct (containsNull = true)
#  |    |    |-- cutomer: string (nullable = true)
#  |    |    |-- date: string (nullable = true)
#  |    |    |-- helpful: string (nullable = true)
#  |    |    |-- rating: string (nullable = true)
#  |    |    |-- votes: string (nullable = true)
#  |-- reviews_summary: struct (nullable = true)
#  |    |-- avg_rating: string (nullable = true)
#  |    |-- downloaded: string (nullable = true)
#  |    |-- total: string (nullable = true)
#  |-- salesrank: string (nullable = true)
#  |-- similar: array (nullable = true)
#  |    |-- element: string (containsNull = true)
#  |-- title: string (nullable = true)
#
# -/-- Schema