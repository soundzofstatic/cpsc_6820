import sys
from pyspark import SparkContext, SparkConf, SQLContext
from pyspark.sql.functions import col, explode

sc = SparkContext("local", "First App")

sqlCtx = SQLContext(sc)
amzDF = sqlCtx.read.json("data/amazon-meta.small.json")
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
        # searchCustomerId = amzDF.withColumn("reviews", explode("reviews")).select("*", col("reviews.*"))\
        searchCustomerId = amzDF.withColumn("reviews", explode("reviews")).select("*", col("reviews.*"))\
            .where(""" rating{}{} """.format(sys.argv[2], sys.argv[3]))

        for i in searchCustomerId.collect(): print("Matched Product: " + i.Id + ", Customer: " + i.cutomer  + ", Rating: " + i.rating + ", title: " + i.title)

except Exception as err:
    print(err)